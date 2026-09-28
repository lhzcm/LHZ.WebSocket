using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Delegates;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Exceptions;
using LHZ.WebSocket.Http;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket
{
    /// <summary>
    /// Represents a WebSocket client that manages the connection, sending, and receiving of WebSocket frames over a TCP stream.
    /// </summary>
    public class WebSocketClient : IWebSocketClient
    {
        private readonly Guid _id;
        /// <summary>
        /// The underlying TCP stream associated with this WebSocket client, used for sending and receiving data frames.
        /// </summary>
        protected readonly Stream _networkStream;
        /// <summary>Bounded channel for outgoing data frames (producer-consumer).</summary>
        protected readonly Channel<DataFrame> _channel;
        /// <summary>
        /// The underlying HTTP context associated with this WebSocket client, which provides access to the TCP stream and request/response details.
        /// </summary>
        protected readonly IHttpContext _httpContext;
        /// <summary>
        /// CancellationTokenSource used to signal background tasks to stop when the client is closed or disposed.
        /// </summary>
        protected readonly CancellationTokenSource _cts = new CancellationTokenSource();
        /// <summary>
        /// Unique identifier for this WebSocket client, used for tracking and managing connections.
        /// </summary>
        public Guid ID => _id;
        /// <summary>
        /// The underlying HTTP context associated with this WebSocket client, which provides access to the TCP stream and request/response details.
        /// </summary>
        public IHttpContext HttpContext => _httpContext;
        /// <summary>Raised when a complete text message is received.</summary>
        public event Delegates.EventHandler<IWebSocketClient, string>? OnMessageReceived;

        /// <summary>Raised when a complete binary message is received.</summary>
        public event Delegates.EventHandler<IWebSocketClient, byte[]>? OnBytesReceived;

        /// <summary>Raised when a close frame is received from the peer.</summary>
        public event Delegates.EventHandler<IWebSocketClient, CloseMessage>? OnCloseReceived;

        /// <summary>Raised when this client disconnects (local or remote).</summary>
        public event Action<IWebSocketClient>? OnClientClose;
        /// <summary>
        /// Raised when a Ping frame is received from the peer.
        /// </summary>
        public event Delegates.EventHandler<IWebSocketClient, byte[]>? OnPingReceived;
        /// <summary>
        /// Raised when a Pong frame is received from the peer.
        /// </summary>
        public event Delegates.EventHandler<IWebSocketClient, byte[]>? OnPongReceived;
        /// <summary>
        /// Raised when a background task fails. When no handler is attached the error is
        /// written to the console instead.
        /// </summary>
        public event Delegates.EventHandler<IWebSocketClient, Exception>? OnError;
        /// <summary>
        /// ClientStatus represents the current state of the WebSocket connection.
        /// Guarded by <see cref="_stateLock"/>; read through <see cref="Status"/>.
        /// </summary>
        protected ClientStatus _clientStatus;

        /// <summary>Guards <see cref="_clientStatus"/> transitions in Open/Close.</summary>
        private readonly object _stateLock = new object();

        /// <summary>
        /// The sender loop. Retained so a graceful close can wait for queued frames to reach
        /// the socket before the connection is torn down.
        /// </summary>
        private Task? _senderTask;

        /// <summary>
        /// True when this client plays the client role (outbound connection).
        /// RFC 6455 §5.1 requires clients to mask every frame they send.
        /// </summary>
        private readonly bool _isClient;

        /// <summary>
        /// Largest reassembled message accepted from the peer, in bytes. Default 4 MiB.
        /// A peer exceeding this is closed with <see cref="CloseCode.MessageTooBig"/>.
        /// Set to 0 to disable the limit.
        /// </summary>
        public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;

        /// <summary>Current connection status.</summary>
        public ClientStatus Status
        {
            get
            {
                lock (_stateLock)
                {
                    return _clientStatus;
                }
            }
        }
        /// <summary>
        /// Initializes a new instance of the WebSocketClient class with the specified HTTP context and channel capacity.
        /// </summary>
        /// <param name="httpContext">The HTTP context associated with the WebSocket connection.</param>
        /// <param name="capacity">The maximum number of data frames that can be queued for sending.</param>
        public WebSocketClient(IHttpContext httpContext, int capacity) : this(httpContext, capacity, false)
        {
        }
        /// <summary>
        /// Initializes a new instance of the WebSocketClient class with the specified HTTP context and channel capacity.
        /// </summary>
        /// <param name="httpContext">The HTTP context associated with the WebSocket connection.</param>
        /// <param name="capacity">The maximum number of data frames that can be queued for sending.</param>
        /// <param name="isClient">True when this connection plays the client role (outgoing frames must be masked).</param>
        internal WebSocketClient(IHttpContext httpContext, int capacity, bool isClient)
        {
            _isClient = isClient;
            _clientStatus = ClientStatus.Connection;
            _httpContext = httpContext;
            _networkStream = httpContext.Stream;
            _channel = Channel.CreateBounded<DataFrame>(capacity);
            _id = Guid.NewGuid();
        }
        /// <summary>
        /// Initializes a new instance of the WebSocketClient class with the specified HTTP context.
        /// </summary>
        /// <param name="httpContext">The HTTP context associated with the WebSocket connection.</param>
        public WebSocketClient(IHttpContext httpContext)
        {
            _clientStatus = ClientStatus.Connection;
            _httpContext = httpContext;
            _networkStream = httpContext.Stream;
            _channel = Channel.CreateBounded<DataFrame>(1024);
            _id = Guid.NewGuid();
        }
        /// <summary>
        /// Creates a new WebSocket client instance.
        /// </summary>
        /// <param name="url">The URL to connect to.</param>
        /// <param name="headers">The HTTP headers for the connection.</param>
        /// <param name="timeOut">The timeout for the connection.</param>
        /// <param name="capacity">The capacity of the message queue.</param>
        /// <returns>The created WebSocket client.</returns>
        /// <exception cref="Exception">Thrown when there is an issue with the URL or connection.</exception>
        public static WebSocketClient CreateWebSocketClient(string url, System.Net.Http.Headers.HttpHeaders? headers = null, int timeOut = 0, int capacity = 1024)
        {
            if(!Uri.TryCreate(url, UriKind.Absolute, out Uri? result) || result == null)
            {
                throw new Exception("There’s a problem with the URL link");
            }
            if (result.Scheme.Equals("wss", StringComparison.OrdinalIgnoreCase) ||
                result.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException("TLS (wss://) is not supported; use ws:// instead.");
            }
            int port = result.Port > 0 ? result.Port : 80;

            // Copy the caller's headers instead of mutating them, so the same collection
            // can be reused across calls without a duplicate "Host" header.
            var requestHeaders = new HttpHeaders();
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    requestHeaders.TryAddWithoutValidation(header.Key, header.Value);
                }
            }
            if (!requestHeaders.Contains("Host"))
            {
                requestHeaders.TryAddWithoutValidation("Host", result.IsDefaultPort ? result.Host : $"{result.Host}:{port}");
            }

            var tcpClient = new TcpClient(result.Host, port);
            try
            {
                using (var httpContext = Http.HttpContext.GetHttpContext(tcpClient, new HttpRequest(result.PathAndQuery, "GET", "HTTP/1.1", requestHeaders), timeOut))
                {
                    return httpContext.HttpUpgrade(capacity);
                }
            }
            catch
            {
                // The handshake failed, so no WebSocketClient owns the socket yet.
                tcpClient.Dispose();
                throw;
            }
        }
        /// <summary>
        /// Sends a UTF-8 text message to the peer.
        /// Blocks if the outgoing queue is full; prefer <see cref="SendMessageAsync"/>.
        /// </summary>
        public void SendMessage(string message)
        {
            Send(OpCode.Text, System.Text.Encoding.UTF8.GetBytes(message));
        }

        /// <summary>Sends a UTF-8 text message to the peer without blocking the caller.</summary>
        public Task SendMessageAsync(string message, CancellationToken cancellationToken = default)
        {
            return SendAsync(OpCode.Text, System.Text.Encoding.UTF8.GetBytes(message), cancellationToken);
        }

        /// <summary>
        /// Sends raw binary data to the peer.
        /// Blocks if the outgoing queue is full; prefer <see cref="SendByteAsync"/>.
        /// <paramref name="bytes">send data, warring: this array will be masked so the value will change</paramref>
        /// </summary>
        public void SendByte(byte[] bytes)
        {
            Send(OpCode.Binary, bytes);
        }

        /// <summary>
        /// Sends raw binary data to the peer without blocking the caller.
        /// <paramref name="bytes">send data, warring: this array will be masked so the value will change</paramref>
        /// </summary>
        public Task SendByteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            return SendAsync(OpCode.Binary, bytes, cancellationToken);
        }

        /// <summary>
        /// Generates the masking key for an outgoing frame: a fresh cryptographically random
        /// key per frame for the client role (RFC 6455 §5.1, §5.3), or 0 for the server role.
        /// </summary>
        private UInt32 NextMaskingKey()
        {
            if (!_isClient)
            {
                return 0;
            }
            UInt32 maskingKey;
            Span<byte> maskingKeyArray = stackalloc byte[4];
            do
            {
#if NET6_0_OR_GREATER
                RandomNumberGenerator.Fill(maskingKeyArray);
#else
                using (var rng = RandomNumberGenerator.Create())
                {
                    rng.GetBytes(maskingKeyArray);
                }
#endif
                maskingKey = DataFrame.MaskingKeyToUint32(ref maskingKeyArray);
            }
            // An all-zero key is indistinguishable from "no key" in the frame factory,
            // which would drop the MASK bit and violate the protocol. Draw again.
            while (maskingKey == 0);
            return maskingKey;
        }

        /// <summary>
        /// Enqueues a data frame onto the outgoing channel.
        /// Blocks the caller while the channel is full.
        /// </summary>
        protected void Send(OpCode opCode, byte[] bytes)
        {
            var dataFrame = DataFrame.CreateDataFrame(opCode, true, bytes, NextMaskingKey());
            _channel.Writer.WriteAsync(dataFrame).GetAwaiter().GetResult();
        }

        /// <summary>Enqueues a data frame onto the outgoing channel without blocking.</summary>
        protected Task SendAsync(OpCode opCode, byte[] bytes, CancellationToken cancellationToken = default)
        {
            var dataFrame = DataFrame.CreateDataFrame(opCode, true, bytes, NextMaskingKey());
            return _channel.Writer.WriteAsync(dataFrame, cancellationToken).AsTask();
        }

        /// <summary>
        /// Queues a Close frame without ever blocking. Called from the receiver task, which
        /// must not stall on a full outgoing queue when the peer has stopped reading.
        /// Failures are reported but otherwise ignored: the connection is already closing.
        /// </summary>
        private void TrySendClose(CloseCode closeCode, bool includeCode = true)
        {
            try
            {
                byte[] payload;
                if (!includeCode)
                {
                    payload = Array.Empty<byte>();
                }
                else
                {
                    payload = new byte[2];
                    payload[0] = (byte)(((int)closeCode >> 8) & 0xFF);
                    payload[1] = (byte)((int)closeCode & 0xFF);
                }
                var dataFrame = DataFrame.CreateDataFrame(OpCode.Close, true, payload, NextMaskingKey());
                _channel.Writer.TryWrite(dataFrame);
            }
            catch (Exception ex)
            {
                ReportError(ex);
            }
        }

        /// <summary>
        /// Surfaces a background failure through <see cref="OnError"/>, falling back to the
        /// console when nothing is subscribed.
        /// </summary>
        private void ReportError(Exception ex)
        {
            var handler = OnError;
            if (handler != null)
            {
                try
                {
                    handler.Invoke(this, ex);
                    return;
                }
                catch (Exception)
                {
                    // A faulty error handler must not mask the original failure.
                }
            }
            Console.WriteLine($"LHZ.WebSocket error: {ex.Message}");
        }

        /// <summary>Starts the reader and sender background tasks. Safe to call more than once.</summary>
        public void Open()
        {
            lock (_stateLock)
            {
                if (_clientStatus != ClientStatus.Connection)
                {
                    return;
                }
                _clientStatus = ClientStatus.Opened;
            }
            StartReceiver();
            StartSender();
        }

        /// <summary>
        /// Cancels background tasks and disposes the underlying TCP connection.
        /// Safe to call concurrently and more than once; the close event fires exactly once.
        /// </summary>
        public void Close()
        {
            // Claim the transition under the lock so concurrent callers (the receiver task,
            // the sender task and WebSocketServer.Stop) cannot double-dispose the socket
            // or raise OnClientClose more than once.
            lock (_stateLock)
            {
                if (_clientStatus == ClientStatus.Close)
                {
                    return;
                }
                _clientStatus = ClientStatus.Close;
            }
            try
            {
                _cts.Cancel();
                if (_httpContext is HttpContext httpContext)
                {
                    httpContext.TcpClient.Close();
                    httpContext.TcpClient.Dispose();
                }
                else
                {
                    _networkStream.Close();
                    _networkStream.Dispose();
                }
            }
            catch (Exception ex)
            {
                ReportError(ex);
            }
            // _cts is deliberately not disposed here: the receiver and sender tasks may still
            // be observing its token, and disposing the source out from under them would turn
            // a clean shutdown into an ObjectDisposedException.

            // Raised outside the lock: handlers may call back into this client.
            OnClientClose?.Invoke(this);
        }

        /// <summary>
        /// Background task that continuously reads WebSocket frames from the network stream,
        /// reassembles fragmented messages, and dispatches them via <see cref="ReceiveProcessing"/>.
        /// </summary>
        private void StartReceiver()
        {
            Task.Run(async () =>
            {
                try
                {
                    var dataFrameReader = new DataFrameReader(_networkStream);
                    List<DataFrame> dataFrames = new List<DataFrame>();
                    long fragmentedLength = 0;
                    await foreach (var item in dataFrameReader.ReadAsync(_cts.Token))
                    {
                        if (_cts.Token.IsCancellationRequested)
                        {
                            break;
                        }

                        ValidateFrame(item);

                        // Control frames (Ping/Pong/Close) must not be fragmented and are
                        // handled immediately; they never participate in message reassembly
                        // (RFC 6455 §5.4, §5.5).
                        if (item.Opcode == OpCode.Ping || item.Opcode == OpCode.Pong || item.Opcode == OpCode.Close)
                        {
                            if (!item.FIN)
                            {
                                throw new WebSocketProtocolException(CloseCode.ProtocolError, "Control frame must not be fragmented");
                            }
                            // A control frame payload is capped at 125 bytes (RFC 6455 §5.5).
                            if (item.Data.Count > 125)
                            {
                                throw new WebSocketProtocolException(CloseCode.ProtocolError, "Control frame payload must not exceed 125 bytes");
                            }
                            ReceiveProcessing(item.Opcode, item.RSV1, item.RSV2, item.RSV3, ToArray(item.Data));
                            continue;
                        }

                        // Continuation frame without a fragmented message in progress is a protocol error.
                        if (item.Opcode == OpCode.Continuation)
                        {
                            if (dataFrames.Count == 0)
                            {
                                throw new WebSocketProtocolException(CloseCode.ProtocolError, "Continuation frame received without a started message");
                            }
                            fragmentedLength += item.Data.Count;
                            EnforceMaxMessageSize(fragmentedLength);
                            dataFrames.Add(item);
                            if (item.FIN)
                            {
                                ReceiveFragmentedMessage(dataFrames);
                                dataFrames.Clear();
                                fragmentedLength = 0;
                            }
                            continue;
                        }

                        // A new Text/Binary frame while a fragmented message is still in progress
                        // is a protocol error (RFC 6455 §5.4).
                        if (dataFrames.Count > 0)
                        {
                            throw new WebSocketProtocolException(CloseCode.ProtocolError, "New data frame received while a fragmented message is in progress");
                        }

                        EnforceMaxMessageSize(item.Data.Count);

                        // Data frame: single-frame message or the start of a fragmented one.
                        if (item.FIN)
                        {
                            ReceiveProcessing(item.Opcode, item.RSV1, item.RSV2, item.RSV3, ToArray(item.Data));
                        }
                        else
                        {
                            fragmentedLength = item.Data.Count;
                            dataFrames.Add(item);
                        }
                    }
                }
                catch (WebSocketProtocolException ex)
                {
                    // Tell the peer why we are failing the connection before tearing it down.
                    ReportError(ex);
                    await CloseAfterDrainAsync(ex.CloseCode, true);
                }
                catch (OperationCanceledException)
                {
                    // Normal shutdown triggered by Close().
                    Close();
                }
                catch (Exception ex)
                {
                    ReportError(ex);
                    Close();
                }
            });
        }

        /// <summary>
        /// Rejects frames that violate RFC 6455 regardless of opcode: reserved bits set with no
        /// negotiated extension, an unknown opcode, or a masking direction that does not match
        /// this endpoint's role.
        /// </summary>
        private void ValidateFrame(DataFrame frame)
        {
            // §5.2: RSV1-3 must be zero unless an extension defining them has been negotiated,
            // and this implementation negotiates none.
            if (frame.RSV1 || frame.RSV2 || frame.RSV3)
            {
                throw new WebSocketProtocolException(CloseCode.ProtocolError, "Reserved bits must be zero: no extension was negotiated");
            }
            switch (frame.Opcode)
            {
                case OpCode.Continuation:
                case OpCode.Text:
                case OpCode.Binary:
                case OpCode.Close:
                case OpCode.Ping:
                case OpCode.Pong:
                    break;
                default:
                    throw new WebSocketProtocolException(CloseCode.ProtocolError, $"Unknown opcode: 0x{(int)frame.Opcode:X}");
            }
            // §5.1: every frame a client sends must be masked, and every frame a server
            // sends must not be.
            if (_isClient)
            {
                if (frame.Masked)
                {
                    throw new WebSocketProtocolException(CloseCode.ProtocolError, "Frames sent by a server must not be masked");
                }
            }
            else if (!frame.Masked)
            {
                throw new WebSocketProtocolException(CloseCode.ProtocolError, "Frames sent by a client must be masked");
            }
        }

        /// <summary>
        /// Fails the connection when an incoming message exceeds <see cref="MaxMessageSize"/>,
        /// so a peer cannot exhaust memory by never setting FIN.
        /// </summary>
        private void EnforceMaxMessageSize(long length)
        {
            int limit = MaxMessageSize;
            if (limit > 0 && length > limit)
            {
                throw new WebSocketProtocolException(CloseCode.MessageTooBig, $"Message exceeds MaxMessageSize ({limit} bytes)");
            }
        }

        /// <summary>Copies a payload segment into a standalone array.</summary>
        private static byte[] ToArray(ArraySegment<byte> segment)
        {
            if (segment.Array == null || segment.Count == 0)
            {
                return Array.Empty<byte>();
            }
            var bytes = new byte[segment.Count];
            Buffer.BlockCopy(segment.Array, segment.Offset, bytes, 0, segment.Count);
            return bytes;
        }

        /// <summary>
        /// Queues a Close frame, stops accepting new frames, and gives the sender task a
        /// short window to flush the queue before the socket is torn down. Without the drain
        /// the close frame would usually be discarded by the immediately following Close().
        /// </summary>
        private async Task CloseAfterDrainAsync(CloseCode closeCode, bool includeCode)
        {
            TrySendClose(closeCode, includeCode);
            _channel.Writer.TryComplete();
            try
            {
                // Wait on the sender *task*, not on the channel. The channel reports itself
                // drained as soon as the last frame has been dequeued, which is before the
                // bytes have been written to the socket; closing then would discard the frame.
                var senderTask = _senderTask;
                if (senderTask != null)
                {
                    await Task.WhenAny(senderTask, Task.Delay(TimeSpan.FromSeconds(5)));
                }
            }
            catch (Exception)
            {
                // Draining is best effort; the connection is going away either way.
            }
            Close();
        }

        /// <summary>
        /// Concatenates the fragments of a completed message and dispatches it once.
        /// </summary>
        private void ReceiveFragmentedMessage(List<DataFrame> dataFrames)
        {
            int count = dataFrames.Sum(n => n.Data.Count);
            var bytes = new byte[count];
            int offset = 0;
            foreach (var dataFrame in dataFrames)
            {
                var segment = dataFrame.Data;
                if (segment.Array != null && segment.Count > 0)
                {
                    Buffer.BlockCopy(segment.Array, segment.Offset, bytes, offset, segment.Count);
                }
                offset += segment.Count;
            }
            ReceiveProcessing(dataFrames[0].Opcode, dataFrames[0].RSV1, dataFrames[0].RSV2, dataFrames[0].RSV3, bytes);
        }

        /// <summary>
        /// Dispatches a fully reassembled message to the appropriate event handler based on opcode.
        /// Override to customize ping/pong handling.
        /// </summary>
        protected virtual void ReceiveProcessing(OpCode opCode, bool RSV1, bool RSV2, bool RSV3, byte[] data)
        {
            switch (opCode)
            {
                case OpCode.Binary: OnBytesReceived?.Invoke(this, data); break;
                case OpCode.Close:
                    {
                        // RFC 6455 §5.5.1: a Close frame payload is either empty
                        // (meaning 1005 No Status Received) or at least 2 bytes long.
                        if (data.Length == 1)
                            throw new WebSocketProtocolException(CloseCode.ProtocolError, "Close frame payload must be empty or at least 2 bytes");
                        CloseMessage closeMessage;
                        bool echoCode = false;
                        CloseCode ackCode = CloseCode.Normal;
                        if (data.Length >= 2)
                        {
                            // First two bytes = close status code (big-endian)
                            CloseCode closeCode = (CloseCode)((data[0] << 8) | data[1]);
                            if (!IsValidIncomingCloseCode(closeCode))
                                throw new WebSocketProtocolException(CloseCode.ProtocolError, $"Close code not allowed on the wire: {(int)closeCode}");
                            closeMessage = new CloseMessage(closeCode, DecodeUtf8(data, 2, data.Length - 2));
                            ackCode = closeCode;
                            echoCode = true;
                        }
                        else
                        {
                            // An empty payload means 1005, which must never be sent back.
                            closeMessage = new CloseMessage(CloseCode.NoStatusReceived, "");
                        }
                        // Auto-reply with a Close frame of our own (RFC 6455 §5.5.1: upon
                        // receiving a Close frame, an endpoint sends a Close frame in response),
                        // before raising the event so user code may Close() right away.
                        OnCloseReceived?.Invoke(this, closeMessage);
                        // Flush the acknowledgement, then tear the connection down. Fire and
                        // forget: this runs on the receiver task, which must not block here.
                        _ = CloseAfterDrainAsync(ackCode, echoCode);
                        break;
                    }
                case OpCode.Text:
                    {
                        // §8.1: invalid UTF-8 in a text message must fail the connection with 1007.
                        var str = DecodeUtf8(data, 0, data.Length);
                        OnMessageReceived?.Invoke(this, str); break;
                    }
                case OpCode.Ping:
                    {
                        // §5.5.2: a Pong must be sent in response to a Ping, carrying the
                        // same payload. Queued without blocking; dropped if the queue is full.
                        try
                        {
                            var pong = DataFrame.CreateDataFrame(OpCode.Pong, true, (byte[])data.Clone(), NextMaskingKey());
                            _channel.Writer.TryWrite(pong);
                        }
                        catch (Exception ex)
                        {
                            ReportError(ex);
                        }
                        OnPingReceived?.Invoke(this, data);
                        break;
                    }
                case OpCode.Pong:
                    {
                        OnPongReceived?.Invoke(this, data);
                        break;
                    }
                default: throw new WebSocketProtocolException(CloseCode.ProtocolError, "OpCode not Support");
            }
        }

        /// <summary>Strict UTF-8 decoder: throws rather than substituting replacement characters.</summary>
        private static readonly UTF8Encoding _strictUtf8 = new UTF8Encoding(false, true);

        /// <summary>
        /// Decodes a UTF-8 payload, failing the connection with
        /// <see cref="CloseCode.InvalidFramePayloadData"/> when the bytes are not valid UTF-8
        /// (RFC 6455 §8.1) instead of silently producing replacement characters.
        /// </summary>
        private static string DecodeUtf8(byte[] data, int index, int count)
        {
            try
            {
                return _strictUtf8.GetString(data, index, count);
            }
            catch (DecoderFallbackException ex)
            {
                throw new WebSocketProtocolException(CloseCode.InvalidFramePayloadData, "Payload is not valid UTF-8", ex);
            }
        }

        /// <summary>
        /// True when a close code may legitimately appear in a Close frame received from a peer.
        /// RFC 6455 §7.4 allows 1000-1003 and 1007-1014, plus the registered range 3000-3999
        /// and the private range 4000-4999. 1004, 1005, 1006 and 1015 are reserved and must
        /// never appear on the wire.
        /// </summary>
        private static bool IsValidIncomingCloseCode(CloseCode closeCode)
        {
            int code = (int)closeCode;
            if (code >= 3000 && code <= 4999)
            {
                return true;
            }
            if (code < 1000 || code > 1014)
            {
                return false;
            }
            return code != (int)CloseCode.Reserved
                && code != (int)CloseCode.NoStatusReceived
                && code != (int)CloseCode.AbnormalClosure;
        }
        /// <summary>
        /// Background task that dequeues outgoing data frames from the channel
        /// and writes them to the network stream.
        /// </summary>
        private void StartSender()
        {
            _senderTask = Task.Run(async () =>
            {
                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        var dataFrame = await _channel.Reader.ReadAsync(_cts.Token);
                        int dataFrameHeaderLength = dataFrame.DataFrameHeaderLength;
                        // use ArayPool reduce GC Collect. The rented array is returned exactly
                        // once, in a finally, and the local is scoped to this iteration so a
                        // later failure can never return the same buffer twice.
                        byte[] dataFrameHeaderBytes = ArrayPool<byte>.Shared.Rent(dataFrameHeaderLength);
                        try
                        {
                            dataFrame.DataFrameHeaderFull(ref dataFrameHeaderBytes);
                            await _networkStream.WriteAsync(dataFrameHeaderBytes, 0, dataFrameHeaderLength, _cts.Token);
                            await _networkStream.WriteAsync(dataFrame.Data, _cts.Token);
                            await _networkStream.FlushAsync(_cts.Token);
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(dataFrameHeaderBytes);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Normal shutdown triggered by Close().
                }
                catch (ChannelClosedException)
                {
                    // The outgoing queue was completed by CloseAfterDrainAsync; nothing left to send.
                }
                catch (Exception ex)
                {
                    ReportError(ex);
                    Close();
                }
            });
        }
        /// <summary>
        /// Sends Ping
        /// </summary>
        /// <param name="bytes"></param>
        public void Ping(byte[] bytes)
        {
            Send(OpCode.Ping, bytes);
        }
        /// <summary>
        /// Sends Ping without blocking the caller.
        /// </summary>
        public Task PingAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            return SendAsync(OpCode.Ping, bytes, cancellationToken);
        }
        /// <summary>
        /// Sends Pong
        /// </summary>
        /// <param name="bytes"></param>
        public void Pong(byte[] bytes)
        {
            Send(OpCode.Pong, bytes);
        }
        /// <summary>
        /// Sends Pong without blocking the caller.
        /// </summary>
        public Task PongAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            return SendAsync(OpCode.Pong, bytes, cancellationToken);
        }
        /// <summary>
        /// Disposes the WebSocket client, closing the connection and releasing resources.
        /// </summary>
        public void Dispose()
        {
            Close();
        }
    }
}
