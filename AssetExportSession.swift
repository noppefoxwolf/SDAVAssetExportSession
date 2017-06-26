//
//  AssetExportSession.swift
//
//
//  Created by Tomoya Hirano on 2017/06/25.
//  Copyright © 2017年 Tomoya Hirano. All rights reserved.
//

import UIKit
import AVFoundation

protocol AssetExportSessionDelegate: class {
  func export(sesseion: AssetExportSession, renderFrame: CVPixelBuffer, with presentationTime: CMTime, to renderBuffer: CVPixelBuffer)
}

final class AssetExportSession {
  struct Error: Swift.Error {
    var domain: String
    var code: AVError.Code
    var description: String
    
    static func make(with error: Swift.Error) -> AssetExportSession.Error {
      return AssetExportSession.Error(domain: "", code: AVError.unknown, description: "")
    }
  }
  
  private let asset: AVAsset
  private let timeRange: CMTimeRange
  private var completionHandler: (()->Void)? = nil
  private var outputURL: URL? = nil
  private var _error: AssetExportSession.Error? = nil
  var error: Swift.Error? {
    if let e = self._error {
      return e
    } else {
      return writer.error ?? reader.error
    }
  }
  var status: AVAssetExportSessionStatus {
    switch writer.status {
    case .unknown: return .unknown
    case .writing: return .exporting
    case .failed: return .failed
    case .cancelled: return .cancelled
    case .completed: return .completed
    }
  }
  
  private var reader: AVAssetReader!
  private var writer: AVAssetWriter!
  var outputFileType: String = AVFileTypeMPEG4
  var shouldOptimizeForNetworkUse: Bool = false
  var metadata = [AVMetadataItem]()
  private var duration: TimeInterval = 0.0
  private var videoOutput: AVAssetReaderVideoCompositionOutput!
  private var videoInput: AVAssetWriterInput!
  private var videoInputSettings = [String : Any]()
  private var videoComposition: AVVideoComposition? = nil
  private var videoSettings: [String : Any]? = nil
  private var audioSettings: [String : Any]? = nil
  private var videoPixelBufferAdaptor: AVAssetWriterInputPixelBufferAdaptor? = nil
  private var audioOutput: AVAssetReaderAudioMixOutput? = nil
  private var audioMix: AVAudioMix? = nil
  private var inputQueue: DispatchQueue? = nil
  private var audioInput: AVAssetWriterInput!
  weak var delegate: AssetExportSessionDelegate? = nil
  
  private var lastSamplePresentationTime: CMTime = kCMTimeZero
  private var progress: Float64 = 0.0
  
  init(with asset: AVAsset) {
    self.asset = asset
    self.timeRange = CMTimeRangeMake(kCMTimeZero, kCMTimePositiveInfinity)
  }
  
  private func exportAsync(withCompletion handler: (()->Void)?) {
    cancel()
    self.completionHandler = handler
    
    guard let outputURL = self.outputURL else {
      _error = AssetExportSession.Error(domain: AVFoundationErrorDomain, code: AVError.exportFailed, description: "Output URL not set")
      handler?()
      return
    }
    
    do {
      reader = try AVAssetReader(asset: self.asset)
    } catch let e {
      _error = AssetExportSession.Error.make(with: e)
      handler?()
      return
    }
    
    do {
      writer = try AVAssetWriter(outputURL: outputURL, fileType: outputFileType)
    } catch let e {
      _error = AssetExportSession.Error.make(with: e)
      handler?()
      return
    }
    reader?.timeRange = timeRange
    writer?.shouldOptimizeForNetworkUse = shouldOptimizeForNetworkUse
    writer?.metadata = metadata
    
    let videoTracks = asset.tracks(withMediaType: AVMediaTypeVideo)
    if CMTIME_IS_INVALID(timeRange.duration) && !CMTIME_IS_POSITIVEINFINITY(timeRange.duration) {
      duration = CMTimeGetSeconds(timeRange.duration)
    } else {
      duration = CMTimeGetSeconds(asset.duration)
    }
    
    //
    // Video output
    //
    if videoTracks.count > 0 {
      videoOutput = AVAssetReaderVideoCompositionOutput(videoTracks: videoTracks, videoSettings: videoInputSettings)
      videoOutput?.alwaysCopiesSampleData = false
      
      if let videoComposition = self.videoComposition {
        videoOutput?.videoComposition = videoComposition
      } else {
        videoOutput?.videoComposition = buildDefaultVideoComposition()
      }
      if reader.canAdd(videoOutput) {
        reader.add(videoOutput)
      }
      
      //
      // Video input
      //
      videoInput = AVAssetWriterInput(mediaType: AVMediaTypeVideo, outputSettings: videoSettings)
      videoInput.expectsMediaDataInRealTime = false
      if writer.canAdd(videoInput) {
        writer.add(videoInput)
      }
      let pixelBufferAttributes: [String : Any] = [
        kCVPixelBufferPixelFormatTypeKey as String : kCVPixelFormatType_32BGRA,
        kCVPixelBufferWidthKey as String : videoOutput.videoComposition!.renderSize.width,
        kCVPixelBufferHeightKey as String : videoOutput.videoComposition!.renderSize.height,
        "IOSurfaceOpenGLESTextureCompatibility" : true,
        "IOSurfaceOpenGLESFBOCompatibility" : true
      ]
      videoPixelBufferAdaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: videoInput, sourcePixelBufferAttributes: pixelBufferAttributes)
    }
    
    //
    //Audio output
    //
    let audioTracks = asset.tracks(withMediaType: AVMediaTypeAudio)
    if audioTracks.count > 0 {
      audioOutput = AVAssetReaderAudioMixOutput(audioTracks: audioTracks, audioSettings: nil)
      audioOutput?.alwaysCopiesSampleData = false
      audioOutput?.audioMix = audioMix
      if reader.canAdd(audioOutput!) {
        reader.add(audioOutput!)
      } else {
        audioOutput = nil
      }
    }
    
    //
    // Audio input
    //
    if audioOutput != nil {
      audioInput = AVAssetWriterInput(mediaType: AVMediaTypeAudio, outputSettings: audioSettings)
      audioInput.expectsMediaDataInRealTime = false
      if writer.canAdd(audioInput) {
        writer.add(audioInput)
      }
    }
    
    writer.startWriting()
    reader.startReading()
    writer.startSession(atSourceTime: timeRange.start)
    
    var videoCompleted = false
    var audioCompleted = false
    inputQueue = DispatchQueue(label: "VideoEncoderInputQueue")
    if videoTracks.count > 0 {
      videoInput.requestMediaDataWhenReady(on: inputQueue!, using: { [weak self] in
        guard let strongSelf = self else { return }
        if !strongSelf.encodeReadySamplesFromOutput(strongSelf.videoOutput, to: strongSelf.videoInput) {
          strongSelf.sync(lock: strongSelf, proc: {
            videoCompleted = true
            if audioCompleted {
              strongSelf.finish()
            }
          })
        }
      })
    } else {
      videoCompleted = true
    }
    
    if let audioOutput = audioOutput {
      audioInput.requestMediaDataWhenReady(on: inputQueue!, using: { [weak self] in
        guard let strongSelf = self else { return }
        if !strongSelf.encodeReadySamplesFromOutput(audioOutput, to: strongSelf.audioInput) {
          strongSelf.sync(lock: strongSelf, proc: {
            audioCompleted = true
            if videoCompleted {
              strongSelf.finish()
            }
          })
        }
      })
    } else {
      audioCompleted = true
    }
  }
  
  func complete() {
    if writer.status == .failed || writer.status == .cancelled {
      try! FileManager.default.removeItem(at: outputURL!)
    }
    completionHandler?()
    completionHandler = nil
  }
  
  func finish() {
    if reader.status == .cancelled || writer.status == .cancelled {
      return
    }
    if writer.status == .failed {
      complete()
    } else if reader.status == .failed {
      writer.cancelWriting()
      complete()
    } else {
      writer.finishWriting(completionHandler: { [weak self] in
        self?.complete()
      })
    }
  }
  
  private func cancel() {
    if let inputQueue = inputQueue {
      inputQueue.async { [weak self] in
        self?.writer.cancelWriting()
        self?.reader.cancelReading()
        self?.complete()
        self?.reset()
      }
    }
  }
  
  func reset() {
    _error = nil
    progress = 0
    reader = nil
    videoOutput = nil
    audioOutput = nil
    writer = nil
    videoInput = nil
    videoPixelBufferAdaptor = nil
    audioInput = nil
    inputQueue = nil
    completionHandler = nil
  }
  
  private func buildDefaultVideoComposition() -> AVVideoComposition? {
    let videoComposition = AVMutableVideoComposition()
    guard let videoTrack = asset.tracks(withMediaType: AVMediaTypeVideo).first else { return nil }
    // get the frame rate from videoSettings, if not set then try to get it from the video track,
    // if not set (mainly when asset is AVComposition) then use the default frame rate of 30
    var trackFrameRate: Float = 0.0
    if let videoSettings = videoSettings {
      if let videoCompressionProperties = videoSettings[AVVideoCompressionPropertiesKey] as? [String : Any] {
        if let frameRate = videoCompressionProperties[AVVideoAverageNonDroppableFrameRateKey] as? Int32 {
          trackFrameRate = Float(frameRate)
        }
      }
    } else {
      trackFrameRate = videoTrack.nominalFrameRate
    }
    
    if trackFrameRate == 0 {
      trackFrameRate = 30
    }
    
    videoComposition.frameDuration = CMTimeMake(1, Int32(trackFrameRate))
    let targetSize = CGSize(width: videoSettings![AVVideoWidthKey] as! Int, height: videoSettings![AVVideoHeightKey] as! Int)
    var naturalSize = videoTrack.naturalSize
    var transform = videoTrack.preferredTransform
    transform.ty = 0
    let videoAngleInDegree = atan2(transform.b, transform.a) * 180 / CGFloat.pi
    if videoAngleInDegree == 90 || videoAngleInDegree == -90 {
      let width = naturalSize.width
      naturalSize.width = naturalSize.height
      naturalSize.height = width
    }
    
    videoComposition.renderSize = naturalSize
    // center inside
    let xRatio = targetSize.width / naturalSize.width
    let yRatio = targetSize.height / naturalSize.height
    let ratio = min(xRatio, yRatio)
    
    let postWidth = naturalSize.width * ratio
    let postHeight = naturalSize.height * ratio
    let transX = (targetSize.width - postWidth) / 2.0
    let transY = (targetSize.height - postHeight) / 2.0
    
    var matrix = CGAffineTransform(translationX: transX / xRatio, y: transY / yRatio)
    matrix = matrix.scaledBy(x: ratio / xRatio, y: ratio / yRatio)
    transform = transform.concatenating(matrix)
    
    // Make a "pass through video track" video composition.
    let passThroughInstruction = AVMutableVideoCompositionInstruction()
    passThroughInstruction.timeRange = CMTimeRangeMake(kCMTimeZero, asset.duration)
    let passThroughLayer = AVMutableVideoCompositionLayerInstruction(assetTrack: videoTrack)
    passThroughLayer.setTransform(transform, at: kCMTimeZero)
    passThroughInstruction.layerInstructions = [passThroughLayer]
    videoComposition.instructions = [passThroughInstruction]
    return videoComposition;
  }
  
  func encodeReadySamplesFromOutput(_ output: AVAssetReaderOutput, to input: AVAssetWriterInput) -> Bool {
    while input.isReadyForMoreMediaData {
      if let sampleBuffer = output.copyNextSampleBuffer() {
        var handled = false
        var error = false
        if reader.status != .reading || writer.status != .writing {
          handled = true
          error = true
        }
        if !handled && videoOutput == output {
          lastSamplePresentationTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
          lastSamplePresentationTime = CMTimeSubtract(lastSamplePresentationTime, timeRange.start)
          progress = duration == 0 ? 1.0 : CMTimeGetSeconds(lastSamplePresentationTime) / duration
          
          let pixelBuffer = CMSampleBufferGetImageBuffer(sampleBuffer)
          var renderBuffer: CVPixelBuffer? = nil
          CVPixelBufferPoolCreatePixelBuffer(nil, videoPixelBufferAdaptor!.pixelBufferPool!, &renderBuffer)
          delegate?.export(sesseion: self, renderFrame: pixelBuffer!, with: lastSamplePresentationTime, to: renderBuffer!)
          if !videoPixelBufferAdaptor!.append(renderBuffer!, withPresentationTime: lastSamplePresentationTime) {
            error = true
          }
          handled = true
        }
        
        if !handled && !input.append(sampleBuffer) {
          error = true
        }
        if error {
          return false
        }
      } else {
        input.markAsFinished()
        return false
      }
    }
    return true
  }
  
  
  
  func sync(lock: AnyObject, proc: () -> ()) {
    objc_sync_enter(lock)
    proc()
    objc_sync_exit(lock)
  }
}



