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
        /// <summary>Incremented by every Start(), identifying which accept loop is current.</summary>
        private int _generation;
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
                    _task = StartWithNewTask(_cancellationTokenSource.Token, ++_generation);
                }
            }
        }

        /// <summary>
        /// Main accept loop: waits for TCP connections, and dispatches each connection
        /// to a background task so a slow handshake never blocks other clients.
        /// </summary>
        private async Task StartWithNewTask(CancellationToken cancellationToken, int generation)
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
                    // A loop from an earlier Start() that finishes late must not mark a
                    // server that has since been started again as closed.
                    if (generation == _generation)
                    {
                        _serverStatus = ServerStatus.Closed;
                    }
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
                if (ex is HttpParseException)
                {
                    DrainAndDispose(tcpClient);
                }
                else
                {
                    tcpClient?.Dispose();
                }
                ReportError(ex);
            }
        }

        /// <summary>
        /// Closes a connection that was refused with an HTTP error response. Closing a socket
        /// that still has unread input makes TCP send a reset, which can destroy the response
        /// before the peer reads it. So: half-close, read off a bounded amount of the leftover
        /// request, then dispose.
        /// </summary>
        private static void DrainAndDispose(TcpClient tcpClient)
        {
            try
            {
                tcpClient.Client.Shutdown(SocketShutdown.Send);
                tcpClient.ReceiveTimeout = 1000;
                var stream = tcpClient.GetStream();
                var buffer = new byte[4096];
                int total = 0;
                int read;
                while (total < 64 * 1024 && (read = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    total += read;
                }
            }
            catch (Exception)
            {
                // The peer is gone or too slow; either way the connection is finished.
            }
            finally
            {
                tcpClient.Dispose();
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
            CancellationTokenSource? cancellationTokenSource;
            Task? task;
            lock (_lock)
            {
                if (_serverStatus != ServerStatus.Start)
                    return;
                _serverStatus = ServerStatus.Closing;
                // Take ownership of this run's state. Once the accept loop reports Closed,
                // Start() may run again and install its own; Stop must not touch that.
                cancellationTokenSource = _cancellationTokenSource;
                task = _task;
                _cancellationTokenSource = null;
                _task = null;
            }
            // Each Close() may wait for its peer to drain, so close the clients side by side
            // instead of one after another.
            var closing = WebSocketClients.Select(item => Task.Run(() => item.Close())).ToArray();
            try
            {
                Task.WaitAll(closing);
            }
            catch (AggregateException)
            {
                // A client failing to close must not keep the listener running.
            }
            cancellationTokenSource?.Cancel();
            try
            {
                task?.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // Accept loop already terminated with an error; ignore.
            }
            cancellationTokenSource?.Dispose();
            // The accept loop's finally normally sets Closed. If it had already faulted, or
            // did not finish inside the timeout, force the transition anyway: leaving the
            // server stuck in Closing would make Start() refuse to run again. Only do so while
            // still Closing: Start() is already allowed once the loop reported Closed.
            lock (_lock)
            {
                if (_serverStatus == ServerStatus.Closing)
                {
                    _serverStatus = ServerStatus.Closed;
                }
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
