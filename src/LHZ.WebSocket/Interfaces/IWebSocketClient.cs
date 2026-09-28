using System.Net.Http.Headers;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Delegates;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Interfaces
{
    /// <summary>
    /// One WebSocket connection: its identity, status, send operations and events.
    /// </summary>
    public interface IWebSocketClient : System.IDisposable
    {
        /// <summary>Raised when a complete text message is received.</summary>
        public event EventHandler<IWebSocketClient, string> OnMessageReceived;
        /// <summary>Raised when a complete binary message is received.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnBytesReceived;
        /// <summary>Raised when a close frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, CloseMessage> OnCloseReceived;
        /// <summary>Raised when a Ping frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnPingReceived;
        /// <summary>Raised when a Pong frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnPongReceived;
        /// <summary>Raised when this client disconnects (local or remote).</summary>
        public event System.Action<IWebSocketClient> OnClientClose;
        /// <summary>
        /// Raised when a background task fails. When no handler is attached the error is
        /// written to the console instead.
        /// </summary>
        public event EventHandler<IWebSocketClient, System.Exception> OnError;
        /// <summary>
        /// Client ID, which is a unique identifier for each WebSocket connection.
        /// </summary>
        public System.Guid ID { get; }
        /// <summary>Current connection status.</summary>
        public ClientStatus Status { get;}
        /// <summary>
        /// Largest reassembled message accepted from the peer, in bytes.
        /// Set to 0 to disable the limit.
        /// </summary>
        public int MaxMessageSize { get; set; }
        /// <summary>
        /// Sends a UTF-8 text message to the peer.
        /// Blocks if the outgoing queue is full; prefer <see cref="SendMessageAsync"/>.
        /// </summary>
        public void SendMessage(string message);
        /// <summary>Sends a UTF-8 text message to the peer without blocking the caller.</summary>
        public System.Threading.Tasks.Task SendMessageAsync(string message, System.Threading.CancellationToken cancellationToken = default);
        /// <summary>
        /// Sends raw binary data to the peer.
        /// Blocks if the outgoing queue is full; prefer <see cref="SendByteAsync"/>.
        /// </summary>
        public void SendByte(byte[] bytes);
        /// <summary>Sends raw binary data to the peer without blocking the caller.</summary>
        public System.Threading.Tasks.Task SendByteAsync(byte[] bytes, System.Threading.CancellationToken cancellationToken = default);
        /// <summary>
        /// Sends Ping
        /// </summary>
        /// <param name="bytes"></param>
        public void Ping(byte[] bytes);
        /// <summary>Sends Ping without blocking the caller.</summary>
        public System.Threading.Tasks.Task PingAsync(byte[] bytes, System.Threading.CancellationToken cancellationToken = default);
        /// <summary>
        /// Sends Pong
        /// </summary>
        public void Pong(byte[] bytes);
        /// <summary>Sends Pong without blocking the caller.</summary>
        public System.Threading.Tasks.Task PongAsync(byte[] bytes, System.Threading.CancellationToken cancellationToken = default);
        /// <summary>Starts the reader and sender background tasks.</summary>
        public void Open();
        /// <summary>Cancels background tasks and disposes the underlying TCP connection.</summary>
        public void Close();
    }
}
