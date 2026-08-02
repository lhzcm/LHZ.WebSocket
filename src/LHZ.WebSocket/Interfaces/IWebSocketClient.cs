using System.Net.Http.Headers;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Delegates;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Interfaces
{
    public interface IWebSocketClient : System.IDisposable
    {
        /// <summary>Raised when a complete text message is received.</summary>
        public event EventHandler<IWebSocketClient, string> OnMessageReceived;
        /// <summary>Raised when a complete binary message is received.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnBytesReceived;
        /// <summary>Raised when a close frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, CloseMessage> OnCloseRecived;
        /// <summary>Raised when a Ping frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnPingRecived;
        /// <summary>Raised when a Pong frame is received from the peer.</summary>
        public event EventHandler<IWebSocketClient, byte[]> OnPongRecived;
        /// <summary>Raised when this client disconnects (local or remote).</summary>
        public event System.Action<IWebSocketClient> OnClientClose;
        /// <summary>
        /// Client ID, which is a unique identifier for each WebSocket connection.
        /// </summary>
        public System.Guid ID { get; }
        /// <summary>Current connection status.</summary>
        public ClientStatus Status { get;}
        /// <summary>Sends a UTF-8 text message to the peer.</summary>
        public void SendMessage(string message);
        /// <summary>Sends raw binary data to the peer.</summary>
        public void SendByte(byte[] bytes);
        /// <summary>
        /// Sends Ping
        /// </summary>
        /// <param name="bytes"></param>
        public void Ping(byte[] bytes);
        /// <summary>
        /// Sends Pong
        /// </summary>
        public void Pong(byte[] bytes);
        /// <summary>Starts the reader and sender background tasks.</summary>
        public void Open();
        /// <summary>Cancels background tasks and disposes the underlying TCP connection.</summary>
        public void Close();
    }
}
