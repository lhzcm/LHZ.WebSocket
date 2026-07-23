
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Wraps an incoming HTTP upgrade request and provides the WebSocket handshake logic.
    /// Created per-connection by <see cref="WebSocketServer"/> and disposed after use.
    /// </summary>
    public sealed class HttpContext : IDisposable
    {
        private HttpRequest _request;
        private HttpHeaders _responseHeaders;
        private WebSocketServer _webSocketServer;
        private TcpClient _tcpClient;
        private WebSocketClient _webSocketClient = null!;

        /// <summary>The upgraded WebSocket client (null before <see cref="HttpUpgrade"/> is called).</summary>
        public WebSocketClient WebSocketClient => _webSocketClient;

        private HttpContext(WebSocketServer webSocketServer, TcpClient tcpClient, HttpRequest request)
        {
            _webSocketServer = webSocketServer;
            _tcpClient = tcpClient;
            _request = request;
            _responseHeaders = new HttpHeaders();
        }

        /// <summary>Parses the HTTP request from the TCP stream and returns a new context.</summary>
        internal static HttpContext GetHttpContext(WebSocketServer webSocketServer, TcpClient tcpClient)
        {
            var httpRequest = new HttpRequest(tcpClient.GetStream());
            return new HttpContext(webSocketServer, tcpClient, httpRequest);
        }
        /// <summary>The parsed HTTP upgrade request.</summary>
        public HttpRequest Request => _request;

        /// <summary>Response headers to send back (e.g., Upgrade, Sec-WebSocket-Accept).</summary>
        public System.Net.Http.Headers.HttpHeaders ResponseHeaders => _responseHeaders;

        /// <summary>
        /// Completes the WebSocket handshake: computes the accept key,
        /// sends HTTP 101 Switching Protocols, and creates a <see cref="WebSocketClient"/>.
        /// </summary>
        public WebSocketClient HttpUpgrade()
        {
            if (_webSocketClient != null)
                return _webSocketClient;

            this.ResponseHeaders.Add("Upgrade", "websocket");
            this.ResponseHeaders.Add("Connection", "Upgrade");

            // Compute Sec-WebSocket-Accept per RFC 6455 Section 4.2.2
            string secWebSocketKey = Request.Headers.GetValues("Sec-WebSocket-Key").First()
                ?? throw new InvalidOperationException("Missing Sec-WebSocket-Key header.");

            var sha1 = Convert.ToBase64String(
                SHA1.HashData(
                    System.Text.Encoding.UTF8.GetBytes(
                        secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));

            this.ResponseHeaders.Add("Sec-WebSocket-Accept", sha1);

            // Write HTTP 101 response
            var networkStream = _tcpClient.GetStream();
            networkStream.Write(System.Text.Encoding.UTF8.GetBytes("HTTP/1.1 101 Switching Protocols\n"));
            var headersStrBuild = new StringBuilder();
            foreach (var item in this.ResponseHeaders)
            {
                headersStrBuild.Append(item.Key);
                headersStrBuild.Append(":");
                headersStrBuild.Append(String.Join(',', item.Value));
                headersStrBuild.Append("\n");
            }
            headersStrBuild.Append("\n");
            networkStream.Write(System.Text.Encoding.UTF8.GetBytes(headersStrBuild.ToString()));
            networkStream.Flush();

            _webSocketClient = new WebSocketClient(_tcpClient);
            _webSocketServer.OnClientConnect(_webSocketClient);
            _webSocketClient.Open();
            return _webSocketClient;
        }

        /// <summary>
        /// Disposes the underlying TCP client if no WebSocket upgrade was performed.
        /// </summary>
        public void Dispose()
        {
            if (_webSocketClient == null)
            {
                _tcpClient.Dispose();
            }
        }
    }
}
