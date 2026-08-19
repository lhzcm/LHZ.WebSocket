using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Delegates;
using LHZ.WebSocket.Enums;
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
        public event Delegates.EventHandler<IWebSocketClient, CloseMessage>? OnCloseRecived;

        /// <summary>Raised when this client disconnects (local or remote).</summary>
        public event Action<IWebSocketClient>? OnClientClose;
        /// <summary>
        /// Raised when a Ping frame is received from the peer.
        /// </summary>
        public event Delegates.EventHandler<IWebSocketClient, byte[]>? OnPingRecived;
        /// <summary>
        /// Raised when a Pong frame is received from the peer.
        /// </summary>
        public event Delegates.EventHandler<IWebSocketClient, byte[]>? OnPongRecived;
        /// <summary>
        /// ClientStatus represents the current state of the WebSocket connection.
        /// </summary>
        protected ClientStatus _clientStatus;

        /// <summary>
        /// True when this client plays the client role (outbound connection).
        /// RFC 6455 §5.1 requires clients to mask every frame they send.
        /// </summary>
        private readonly bool _isClient;

        /// <summary>Current connection status.</summary>
        public ClientStatus Status => _clientStatus;
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
        /// <param name="timeOUt">The timeout for the connection.</param>
        /// <param name="capacity">The capacity of the message queue.</param>
        /// <returns>The created WebSocket client.</returns>
        /// <exception cref="Exception">Thrown when there is an issue with the URL or connection.</exception>
        public static WebSocketClient CreateWebSocketClient(string url, System.Net.Http.Headers.HttpHeaders? headers = null, int timeOUt = 0, int capacity = 1024)
        {
            if(!Uri.TryCreate(url, UriKind.Absolute, out Uri? result) || result == null)
            {
                throw new Exception("There’s a problem with the URL link");
            }
            var tcpClient = new TcpClient(result.Host, result.Port);
            if(headers == null)
            {
                headers = new HttpHeaders();
            }
            headers.Add("Host", result.Host);
            using(var httpContext = Http.HttpContext.GetHttpContext(tcpClient, new HttpRequest(result.PathAndQuery, "GET", "HTTP/1.1", headers), timeOUt))
            {
                return httpContext.HttpUpgrade(capacity);
            }
        }
        /// <summary>Sends a UTF-8 text message to the peer.</summary>
        public void SendMessage(string message)
        {
            Send(OpCode.Text, System.Text.Encoding.UTF8.GetBytes(message));
        }

        /// <summary>Sends raw binary data to the peer.</summary>
        public void SendByte(byte[] bytes)
        {
            Send(OpCode.Binary, bytes);
        }

        /// <summary>Enqueues a data frame onto the outgoing channel.</summary>
        protected void Send(OpCode opCode, byte[] bytes)
        {
            byte[]? maskingKey = null;
            if (_isClient)
            {
                // RFC 6455 §5.1: a client MUST mask every frame it sends.
                // Use a cryptographic RNG so the key is not predictable (§5.3).
                maskingKey = new byte[4];
#if NET6_0_OR_GREATER
                RandomNumberGenerator.Fill(maskingKey);
#else
                using (var rng = RandomNumberGenerator.Create())
                {
                    rng.GetBytes(maskingKey);
                }
#endif
            }
            var dataFrame = DataFrame.CreateDataFrame(opCode, true, maskingKey, bytes);
            _channel.Writer.WriteAsync(dataFrame).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Sends a Close frame in reply to a peer's Close frame.
        /// Failures are swallowed: the connection is already closing.
        /// </summary>
        private void SendCloseAck(byte[] payload)
        {
            try
            {
                Send(OpCode.Close, payload);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending close ack: {ex.Message}");
            }
        }

        /// <summary>Starts the reader and sender background tasks.</summary>
        public void Open()
        {
            if (_clientStatus == ClientStatus.Connection)
            {
                _clientStatus = ClientStatus.Opend;
                StartReceiver();
                StartSender();
            }
        }

        /// <summary>Cancels background tasks and disposes the underlying TCP connection.</summary>
        public void Close()
        {
            if (_clientStatus == ClientStatus.Close)
            {
                return;
            }
            _cts.Cancel();
            if(_httpContext is HttpContext httpContext)
            {
                httpContext.TcpClient.Close();
                httpContext.TcpClient.Dispose();
            }
            else
            {
                _networkStream.Close();
                _networkStream.Dispose();
            }
            _clientStatus = ClientStatus.Close;
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
                    await foreach (var item in dataFrameReader.ReadAsync(_cts.Token))
                    {
                        if (_cts.Token.IsCancellationRequested)
                        {
                            break;
                        }

                        // Control frames (Ping/Pong/Close) must not be fragmented and are
                        // handled immediately; they never participate in message reassembly
                        // (RFC 6455 §5.4, §5.5).
                        if (item.Opcode == OpCode.Ping || item.Opcode == OpCode.Pong || item.Opcode == OpCode.Close)
                        {
                            if (!item.FIN)
                            {
                                throw new Exception("Control frame must not be fragmented");
                            }
                            ReceiveProcessing(item.Opcode, item.RSV1, item.RSV2, item.RSV3, item.Data.ToArray());
                            continue;
                        }

                        // Continuation frame without a fragmented message in progress is a protocol error.
                        if (item.Opcode == OpCode.Continuation)
                        {
                            if (dataFrames.Count == 0)
                            {
                                throw new Exception("Continuation frame received without a started message");
                            }
                            dataFrames.Add(item);
                            if (item.FIN)
                            {
                                ReceiveFragmentedMessage(dataFrames);
                                dataFrames.Clear();
                            }
                            continue;
                        }

                        // A new Text/Binary frame while a fragmented message is still in progress
                        // is a protocol error (RFC 6455 §5.4).
                        if (dataFrames.Count > 0)
                        {
                            throw new Exception("New data frame received while a fragmented message is in progress");
                        }

                        // Data frame: single-frame message or the start of a fragmented one.
                        if (item.FIN)
                        {
                            ReceiveProcessing(item.Opcode, item.RSV1, item.RSV2, item.RSV3, item.Data.ToArray());
                        }
                        else
                        {
                            dataFrames.Add(item);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error receiving data: {ex.Message}");
                    Close();
                }
            });
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
                Array.Copy(dataFrame.Data.ToArray(), 0, bytes, offset, dataFrame.Data.Count);
                offset += dataFrame.Data.Count;
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
                            throw new Exception("Close Frame has Error");
                        CloseMessage closeMessage;
                        if (data.Length >= 2)
                        {
                            // First two bytes = close status code (big-endian)
                            CloseCode closeCode = (CloseCode)((data[0] << 8) | data[1]);
                            if (!Enum.IsDefined<CloseCode>(closeCode))
                                throw new Exception("CloseCode has not define");
                            closeMessage = new CloseMessage(closeCode, Encoding.UTF8.GetString(data, 2, data.Length - 2));
                        }
                        else
                        {
                            closeMessage = new CloseMessage(CloseCode.NoStatusReceived, "");
                        }
                        // Auto-reply with a Close frame of our own (RFC 6455 §5.5.1: upon
                        // receiving a Close frame, an endpoint sends a Close frame in response),
                        // before raising the event so user code may Close() right away.
                        SendCloseAck(data);
                        OnCloseRecived?.Invoke(this, closeMessage);
                        break;
                    }
                case OpCode.Text:
                    {
                        var str = System.Text.Encoding.UTF8.GetString(data);
                        OnMessageReceived?.Invoke(this, str); break;
                    }
                case OpCode.Ping:
                    {
                        OnPingRecived?.Invoke(this, data);
                        break;
                    }
                case OpCode.Pong:
                    {
                        OnPongRecived?.Invoke(this, data);
                        break;
                    }
                default: throw new Exception("OpCode not Support");
            }
        }
        /// <summary>
        /// Background task that dequeues outgoing data frames from the channel
        /// and writes them to the network stream.
        /// </summary>
        private void StartSender()
        {
            Task.Run(async () =>
            {
                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        var dataFrame = await _channel.Reader.ReadAsync(_cts.Token);
                        await _networkStream.WriteAsync(dataFrame.DataFrameHeader, _cts.Token);
                        await _networkStream.WriteAsync(dataFrame.Data, _cts.Token);
                        await _networkStream.FlushAsync(_cts.Token);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error Sending data: {ex.Message}");
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
        /// Sends Pong
        /// </summary>
        /// <param name="bytes"></param>
        public void Pong(byte[] bytes)
        {
            Send(OpCode.Pong, bytes);
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
