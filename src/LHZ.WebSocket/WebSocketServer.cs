using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Http;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket
{
    /// <summary>
    /// A lightweight WebSocket server that accepts TCP connections,
    /// handles the HTTP upgrade handshake, and manages connected clients.
    /// </summary>
    public class WebSocketServer
    {
        private IPAddress _ip;
        private int _port;
        private ServerStatus _serverStatus = ServerStatus.Ready;
        private readonly object _lock = new object();
        private CancellationTokenSource? _cancellationTokenSource;
        private Task? _task;
        private int _timeOut = 10;

        /// <summary>Raised when an HTTP upgrade request is received, before the handshake completes.</summary>
        public event Action<HttpContext>? OnUpgradeRequest;

        /// <summary>Raised after a client completes the WebSocket handshake and is ready.</summary>
        public event Action<IWebSocketClient>? OnClientConnected;

        /// <summary>
        /// Raised when the accept loop or a handshake fails. When no handler is attached the
        /// error is written to the console instead.
        /// </summary>
        public event Action<Exception>? OnError;

        /// <summary>Current server status.</summary>
        public ServerStatus Status
        {
            get
            {
                lock (_lock)
                {
                    return _serverStatus;
                }
            }
        }

        /// <summary>Clients connected to this server instance.</summary>
        private readonly HashSet<IWebSocketClient> _webSocketClients = new HashSet<IWebSocketClient>();

        /// <summary>Current number of connected clients.</summary>
        public int ClientNums
        {
            get
            {
                lock (_lock)
                {
                    return _webSocketClients.Count;
                }
            }
        }

        /// <summary>Snapshot of all currently connected clients.</summary>
        public IEnumerable<IWebSocketClient> WebSocketClients
        {
            get
            {
                lock (_lock)
                {
                    return _webSocketClients.ToArray();
                }
            }
        }
        /// <summary>Binds to a specific IP and port with a handshake timeout.</summary>
        /// <param name="ip">The local address to listen on.</param>
        /// <param name="port">The local port to listen on.</param>
        /// <param name="timeOut">Handshake timeout in seconds; 0 or less disables it.</param>
        public WebSocketServer(IPAddress ip, int port, int timeOut)
        {
            _ip = ip;
            _port = port;
            _timeOut = timeOut;
        }
        /// <summary>Binds to a specific IP and port with the default 10 second handshake timeout.</summary>
        /// <param name="ip">The local address to listen on.</param>
        /// <param name="port">The local port to listen on.</param>
        public WebSocketServer(IPAddress ip, int port)
        {
            _ip = ip;
            _port = port;
        }

        /// <summary>Binds to all interfaces on the given port.</summary>
        /// <param name="port">The local port to listen on.</param>
        public WebSocketServer(int port)
        {
            _ip = IPAddress.Any;
            _port = port;
        }

        /// <summary>
        /// Starts the WebSocket server and listens for incoming connections.
        /// </summary>
        public void Start()
        {
            lock (_lock)
            {
                if (_serverStatus == ServerStatus.Ready || _serverStatus == ServerStatus.Closed)
                {
                    _serverStatus = ServerStatus.Start;
                    _cancellationTokenSource = new CancellationTokenSource();
                    _task = StartWithNewTask(_cancellationTokenSource.Token);
                }
            }
        }

        /// <summary>
        /// Main accept loop: waits for TCP connections, and dispatches each connection
        /// to a background task so a slow handshake never blocks other clients.
        /// </summary>
        private async Task StartWithNewTask(CancellationToken cancellationToken)
        {
            TcpListener? listener = null;
            try
            {
                listener = new TcpListener(_ip, _port);
                listener.Start();

                while (!cancellationToken.IsCancellationRequested)
                {
#if NET6_0_OR_GREATER
                    TcpClient tcpClient = await listener.AcceptTcpClientAsync(cancellationToken);
#else
                    TcpClient tcpClient = await listener.AcceptTcpClientAsync();
#endif
                    // Handle the handshake on a separate task: a client that connects
                    // but never completes the HTTP upgrade must not stall the accept loop.
                    _ = Task.Run(() => HandleConnection(tcpClient), cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown triggered by Stop().
            }
            catch (Exception ex)
            {
                ReportError(ex);
            }
            finally
            {
#if NET8_0_OR_GREATER
                listener?.Dispose();
#else
                listener?.Stop();
#endif
                lock (_lock)
                {
                    _serverStatus = ServerStatus.Closed;
                }
            }
        }

        /// <summary>
        /// Parses the HTTP upgrade request for one connection, fires <see cref="OnUpgradeRequest"/>,
        /// and starts the WebSocket client if the user accepted the upgrade.
        /// </summary>
        private void HandleConnection(TcpClient tcpClient)
        {
            try
            {
                using (var httpContext = HttpContext.GetHttpContext(tcpClient, _timeOut))
                {
                    OnUpgradeRequest?.Invoke(httpContext);
                    if (httpContext.WebSocketClient != null)
                    {
                        OnClientConnect(httpContext.WebSocketClient);
                        httpContext.WebSocketClient.Open();
                        // Raised after the handshake completed and the client is reading,
                        // so a handler may send straight away.
                        OnClientConnected?.Invoke(httpContext.WebSocketClient);
                    }
                }
            }
            catch (Exception ex)
            {
                tcpClient?.Dispose();
                ReportError(ex);
            }
        }

        /// <summary>
        /// Surfaces a failure through <see cref="OnError"/>, falling back to the console when
        /// nothing is subscribed.
        /// </summary>
        private void ReportError(Exception ex)
        {
            var handler = OnError;
            if (handler != null)
            {
                try
                {
                    handler.Invoke(ex);
                    return;
                }
                catch (Exception)
                {
                    // A faulty error handler must not mask the original failure.
                }
            }
            Console.WriteLine($"LHZ.WebSocket server error: {ex.Message}{Environment.NewLine}{ex.StackTrace}");
        }

        /// <summary>
        /// Closes all client connections and stops the accept loop.
        /// </summary>
        public void Stop()
        {
            lock (_lock)
            {
                if (_serverStatus != ServerStatus.Start)
                    return;
                _serverStatus = ServerStatus.Closing;
            }
            foreach (var item in WebSocketClients)
            {
                item.Close();
            }
            _cancellationTokenSource?.Cancel();
            try
            {
                _task?.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // Accept loop already terminated with an error; ignore.
            }
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;
            _task = null;
            // The accept loop's finally normally sets Closed. If it had already faulted, or
            // did not finish inside the timeout, force the transition anyway: leaving the
            // server stuck in Closing would make Start() refuse to run again.
            lock (_lock)
            {
                _serverStatus = ServerStatus.Closed;
            }
        }

        /// <summary>Registers a newly upgraded client and subscribes to its close event.</summary>
        internal void OnClientConnect(IWebSocketClient client)
        {
            lock (_lock)
            {
                client.OnClientClose += OnClientClose;
                _webSocketClients.Add(client);
            }
        }

        /// <summary>Removes a disconnected client from the active set.</summary>
        internal void OnClientClose(IWebSocketClient client)
        {
            lock (_lock)
            {
                client.OnClientClose -= OnClientClose;
                _webSocketClients.Remove(client);
            }
        }
    }
}
