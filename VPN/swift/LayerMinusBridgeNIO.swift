//
//  LayerMinusBridgeNIO.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-17.
//

import Foundation
import NIO
import NIOCore
import NIOTransportServices
import NIOConcurrencyHelpers

// MARK: - Piping handler (inbound only) with automatic backpressure
final class DuplexPipe: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private weak var peer: Channel?
    private let name: String
    private let onFirstDownBytes: ((Int) -> Void)?

    init(peer: Channel? = nil, name: String, onFirstDownBytes: ((Int) -> Void)? = nil) {
        self.peer = peer
        self.name = name
        self.onFirstDownBytes = onFirstDownBytes
    }

    func bindPeer(_ ch: Channel) { self.peer = ch }

    func handlerAdded(context: ChannelHandlerContext) {
        // 背压水位：低/高阈值（可在运行时通过 ChannelOption 重新设置，以实现“二层 role 切换”）
        _ = context.channel.setOption(.writeBufferWaterMark,
                                      value: .init(low: 32 * 1024, high: 256 * 1024))
        _ = context.channel.setOption(.autoRead, value: true)
        _ = context.channel.setOption(.allowRemoteHalfClosure, value: true)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard let peer = peer else { return }
        var buf = unwrapInboundIn(data)
        let readable = buf.readableBytes
        if readable > 0, let onFirstDownBytes {
            onFirstDownBytes(readable) // 用于下行首字节 KPI
        }
        peer.writeAndFlush(buf, promise: nil)
    }

    // 自动背压：对端不可写 -> 暂停我方读；恢复可写 -> 继续读
    // 直接实现专用回调，避免依赖事件类型名（各 NIO 版本通用）
    func channelWritabilityChanged(context: ChannelHandlerContext) {
        let writable = context.channel.isWritable
        _ = context.channel.setOption(.autoRead, value: writable)
        context.fireChannelWritabilityChanged()
    }

    func channelInactive(context: ChannelHandlerContext) {
        peer?.close(promise: nil)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        peer?.close(promise: nil)
        context.close(promise: nil)
    }
}

// MARK: - LayerMinusBridgeNIO
public final class LayerMinusBridgeNIO {
    // Immutable
    public let id: UInt64
    public let targetHost: String
    public let targetPort: Int
    public let verbose: Bool
    public let connectInfo: String?
    public let onClosed: ((UInt64) -> Void)?

    // NIO
    private let group = NIOTSEventLoopGroup()
    private var downCh: Channel?   // 下游（客户端）NIO Channel
    private var upCh: Channel?     // 上游（远端）NIO Channel

    // Runtime state
    private var firstByteWatchdog: RepeatedTask?
    private var closed = NIOLockedValueBox(false)

    // KPI
    private var tStart: NIODeadline = .now()
    private var tHandoff: NIODeadline?
    private var tReady: NIODeadline?
    private var tFirstSend: NIODeadline?
    private var tFirstByte: NIODeadline?
    private var bytesUp: Int = 0
    private var bytesDown: Int = 0

    // Role（影响水位，等价于你原来的“二层role”）
    public enum Role { case upstreamHeavy, downstreamHeavy }
    private(set) var role: Role = .upstreamHeavy

    // MARK: Init / Deinit
    public init(
        id: UInt64,
        targetHost: String,
        targetPort: Int,
        verbose: Bool = false,
        connectInfo: String? = nil,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.targetHost = targetHost
        self.targetPort = targetPort
        self.verbose = verbose
        self.connectInfo = connectInfo
        self.onClosed = onClosed
        log("🟢 CREATED LayerMinusBridgeNIO #\(id) for \(targetHost):\(targetPort)\(infoTag())")
    }

    deinit {
        try? group.syncShutdownGracefully()
        log("🔵 DEINIT LayerMinusBridgeNIO #\(id)")
    }

    // MARK: Public API（与旧类对齐）
    public func markHandoffNow() {
        tHandoff = .now()
    }

    /// 供外部在 listener 接收到下游后调用
    public func attachDownstream(_ channel: Channel) {
        self.downCh = channel
        _ = channel.setOption(.allowRemoteHalfClosure, value: true)
    }

    /// 启动：需要保证在此之前已经 attachDownstream(_:)
    public func start(withFirstBody firstBodyBase64: String) {
        guard let down = downCh else {
            log("❌ start() called before attachDownstream()")
            cancel(reason: "no_downstream_channel")
            return
        }
        let alreadyClosed = closed.withLockedValue { $0 }
        guard alreadyClosed == false else { return }

        tStart = .now()
        if let th = tHandoff {
            let ms = Int((tStart - th).nanoseconds / 1_000_000)
            log("KPI handoff_to_start_ms=\(ms)")
        }

        guard let firstBody = Data(base64Encoded: firstBodyBase64) else {
            log("firstBody base64 decode failed")
            cancel(reason: "invalid_first_body")
            return
        }
        if !firstBody.isEmpty {
            let preview = firstBody.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
            log("firstBody decoded bytes=\(firstBody.count), preview: \(preview)")
        }

        // 1) 连接上游
        connectUpstream(on: down.eventLoop).whenComplete { [weak self] res in
            guard let self = self else { return }
            switch res {
            case .failure(let err):
                self.log("upstream connect failed: \(err)")
                self.cancel(reason: "connect_failed")
            case .success(let up):
                self.upCh = up
                self.tReady = .now()
                self.log("upstream ready \(self.targetHost):\(self.targetPort)")

                // 2) 先发首包（如有）
                self.sendFirstBodyIfNeeded(firstBody, via: up).whenComplete { _ in
                    if self.closed.withLockedValue({ $0 }) { return }
                    // 3) 设置 watchdog（首字节）
                    self.setupFirstByteWatchdog(on: down.eventLoop)

                    // 4) 双向 pipe
                    self.installPipes(down: down, up: up)
                }
            }
        }
    }

    public func cancel(reason: String) {
        // 原子交换：若此前已为 true，直接返回
        let wasClosed = closed.withLockedValue { (v: inout Bool) -> Bool in
            if v { return true } else { v = true; return false }
        }
        if wasClosed { return }
        cancelWatchdog()

        let now = NIODeadline.now()
        let durMs = Int((now - tStart).nanoseconds / 1_000_000)
        var extra = ""
        if let fb = tFirstByte {
            let fbMs = Int((fb - tStart).nanoseconds / 1_000_000)
            extra += " first_byte_ms=\(fbMs)"
        }
        log("KPI host=\(targetHost):\(targetPort) reason=\(reason) up_bytes=\(bytesUp) down_bytes=\(bytesDown) dur_ms=\(durMs)\(extra)")

        upCh?.close(mode: .all, promise: nil)
        downCh?.close(mode: .all, promise: nil)
        onClosed?(id)
        log("CANCEL trigger id=\(id) reason=\(reason)")
    }

    /// 运行时切换“二层 role”，动态调整水位（=内存预算）
    public func switchRole(_ newRole: Role) {
        role = newRole
        let (downLow, downHigh, upLow, upHigh): (Int,Int,Int,Int) =
            (newRole == .upstreamHeavy)
            ? (4*1024,  32*1024,  64*1024, 512*1024)   // 下游紧，上游宽（上行重）
            : (64*1024, 512*1024, 4*1024,  32*1024)   // 下游宽，上游紧（下行重）

        if let d = downCh {
            _ = d.setOption(.writeBufferWaterMark, value: .init(low: downLow, high: downHigh))
        }
        if let u = upCh {
            _ = u.setOption(.writeBufferWaterMark, value: .init(low: upLow, high: upHigh))
        }
    }

    // MARK: Private – Connect upstream
    private func connectUpstream(on loop: EventLoop) -> EventLoopFuture<Channel> {
        let bootstrap = NIOTSConnectionBootstrap(group: group)
            .channelOption(.allowRemoteHalfClosure, value: true)

        return bootstrap.connect(host: targetHost, port: targetPort)
    }

    // MARK: Private – First body & watchdog
    private func sendFirstBodyIfNeeded(_ data: Data, via up: Channel) -> EventLoopFuture<Void> {
        guard !data.isEmpty else { return up.eventLoop.makeSucceededFuture(()) }
        var buf = up.allocator.buffer(capacity: data.count)
        buf.writeBytes(data)
        tFirstSend = .now()
        return up.writeAndFlush(buf).map {
            self.log("sent firstBody")
        }.flatMapError { error in
            self.cancel(reason: "firstBody send error: \(error)")
            return up.eventLoop.makeSucceededFuture(())
        }
    }

    private func setupFirstByteWatchdog(on loop: EventLoop) {
        cancelWatchdog()

        var timeoutSec = ProcessInfo.processInfo.environment["VPN_FIRST_BYTE_TIMEOUT"]
            .flatMap(Double.init) ?? 60.0
        if targetHost.hasSuffix("telegram.org") || targetHost.hasPrefix("149.154.") {
            timeoutSec = 90.0 // 75–90 秒取中
        }

        firstByteWatchdog = loop.scheduleRepeatedAsyncTask(initialDelay: .seconds(Int64(timeoutSec)),
                                                           delay: .hours(24)) { [weak self] _ in
            guard let self = self else { return loop.makeSucceededFuture(()) }
            if self.tFirstByte == nil && self.closed.withLockedValue({ $0 }) == false {
                self.cancel(reason: "first_byte_timeout after \(Int(timeoutSec))s")
            }
            return loop.makeSucceededFuture(())
        }
    }

    private func cancelWatchdog() {
        firstByteWatchdog?.cancel()
        firstByteWatchdog = nil
    }

    // MARK: Private – Pipe & KPI
    private func installPipes(down: Channel, up: Channel) {
        let onFirstDown: (Int) -> Void = { [weak self] n in
            guard let self = self else { return }
            if self.tFirstByte == nil {
                self.tFirstByte = .now()
                let ttfb = Int(((self.tFirstByte ?? .now()) - self.tStart).nanoseconds / 1_000_000)
                self.log("KPI immediate TTFB_ms=\(ttfb)")
                if let ts = self.tFirstSend {
                    let seg = Int(((self.tFirstByte ?? .now()) - ts).nanoseconds / 1_000_000)
                    self.log("KPI firstSend_to_firstRecv_ms=\(seg)")
                }
            }
            self.bytesDown &+= n
        }

        let a = DuplexPipe(name: "down->up")                 // 下游 → 上游
        let b = DuplexPipe(name: "up->down", onFirstDownBytes: onFirstDown) // 上游 → 下游

        a.bindPeer(up); b.bindPeer(down)

        // 把 handler 装到 pipeline 末端
        let f1 = down.pipeline.addHandlers([a], position: .last)
        let f2 = up.pipeline.addHandlers([b], position: .last)

        _ = f1.and(f2).map { _ in
            self.log("pipes installed")
        }

        // 在下游通道末尾追加一个简单的出站统计处理器
        down.pipeline.addHandler(ByteCountOutbound(selfRef: self), position: .last).whenComplete { _ in }

    }

    private final class ByteCountOutbound: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer
        private weak var selfRef: LayerMinusBridgeNIO?

        init(selfRef: LayerMinusBridgeNIO?) { self.selfRef = selfRef }

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            var buf = self.unwrapOutboundIn(data)
            let n = buf.readableBytes
            selfRef?.bytesUp &+= n
            context.write(data, promise: promise)
        }
    }

    // MARK: Logging helpers
    @inline(__always)
    private func infoTag() -> String {
        guard let s = connectInfo, !s.isEmpty else { return "" }
        return " [\(s)]"
    }

    @inline(__always)
    private func log(_ msg: @autoclosure () -> String) {
        #if DEBUG
        NSLog("[LayerMinusBridgeNIO \(id)\(infoTag())] %@", msg())
        #endif
    }
}
