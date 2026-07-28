using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
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

        /// <summary>Raised when an HTTP upgrade request is received, before the handshake completes.</summary>
        public event Action<HttpContext>? OnUpgradeRequest;

        /// <summary>Raised after a client completes the WebSocket handshake and is ready.</summary>
        public event Action<IWebSocketClient>? OnClientConnected;

        private static readonly HashSet<IWebSocketClient> _webSocketClients = new HashSet<IWebSocketClient>();
        private CancellationTokenSource? _cancellationTokenSource;
        private Task? _task;
        private int _timeOut = 10;

        /// <summary>Current number of connected clients.</summary>
        public int ClientNums => _webSocketClients.Count;

        /// <summary>Snapshot of all currently connected clients.</summary>
        public IEnumerable<IWebSocketClient> WebSocketClients
        {
            get
            {
                lock (this)
                {
                    return _webSocketClients.ToArray();
                }
            }
        }
        public WebSocketServer(IPAddress ip, int port, int timeOut)
        {
            _ip = ip;
            _port = port;
            _timeOut = timeOut;
        }
        public WebSocketServer(IPAddress ip, int port)
        {
            _ip = ip;
            _port = port;
        }

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
            if (_serverStatus == ServerStatus.Ready || _serverStatus == ServerStatus.Closed)
            {
                _serverStatus = ServerStatus.Start;
                _cancellationTokenSource = new CancellationTokenSource();
                _task = StartWithNewTask(_cancellationTokenSource.Token);
            }
        }

        /// <summary>
        /// Main accept loop: waits for TCP connections, parses HTTP upgrade requests,
        /// and fires <see cref="OnUpgradeRequest"/> for each incoming client.
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
                    try
                    {
                        using (var httpContext = HttpContext.GetHttpContext(tcpClient, _timeOut))
                        {
                            OnUpgradeRequest?.Invoke(httpContext);
                            if (httpContext.WebSocketClient != null)
                            {
                                this.OnClientConnect(httpContext.WebSocketClient);
                                httpContext.WebSocketClient.Open();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        tcpClient?.Dispose();
                        Console.WriteLine(ex.Message + ex.StackTrace);
                    }
                    // var task = Task.Run(() =>
                    // {
                    //     try
                    //     {
                    //         using (var httpContext = HttpContext.GetHttpContext(tcpClient))
                    //         {
                    //             OnUpgradeRequest?.Invoke(httpContext);
                    //             if (httpContext.WebSocketClient != null)
                    //             {
                    //                 this.OnClientConnect(httpContext.WebSocketClient);
                    //                 httpContext.WebSocketClient.Open();
                    //             }
                    //         }
                    //     }
                    //     catch (Exception ex)
                    //     {
                    //         tcpClient?.Dispose();
                    //         Console.WriteLine(ex.Message + ex.StackTrace);
                    //     }
                    // });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + ex.StackTrace);
            }
#if NET8_0_OR_GREATER
            listener?.Dispose();
#else
            listener?.Stop();
#endif
            _serverStatus = ServerStatus.Closed;
        }

        /// <summary>
        /// Closes all client connections and stops the accept loop.
        /// </summary>
        public void Stop()
        {
            if (_serverStatus != ServerStatus.Start)
                return;
            _serverStatus = ServerStatus.Closing;
            foreach (var item in WebSocketClients)
            {
                item.Close();
            }
            _cancellationTokenSource!.Cancel();
        }

        /// <summary>Registers a newly upgraded client and subscribes to its close event.</summary>
        internal void OnClientConnect(IWebSocketClient client)
        {
            lock (this)
            {
                client.OnClientClose += OnClientClose;
                _webSocketClients.Add(client);
            }
        }

        /// <summary>Removes a disconnected client from the active set.</summary>
        internal void OnClientClose(IWebSocketClient client)
        {
            lock (this)
            {
                _webSocketClients.Remove(client);
            }
        }
    }
}
