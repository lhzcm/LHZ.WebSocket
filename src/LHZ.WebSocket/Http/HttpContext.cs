
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
        private HttpResponse? _response;
        private TcpClient _tcpClient;
        private WebSocketClient _webSocketClient = null!;

        /// <summary>The upgraded WebSocket client (null before <see cref="HttpUpgrade"/> is called).</summary>
        public WebSocketClient WebSocketClient => _webSocketClient;
        public TcpClient TcpClient => _tcpClient;

        private HttpContext(TcpClient tcpClient, HttpRequest request, HttpResponse? response)
        {
            _tcpClient = tcpClient;
            _request = request;
            _response = response;
        }

        /// <summary>Parses the HTTP request from the TCP stream and returns a new context.</summary>
        internal static HttpContext GetHttpContext(TcpClient tcpClient, HttpRequest request)
        {
            return new HttpContext(tcpClient, request, null);
        }
        internal static HttpContext GetHttpContext(TcpClient tcpClient)
        {
            var request = HttpRequest.GetRequestFromStream(tcpClient.GetStream());
            return new HttpContext(tcpClient, request, new HttpResponse(HttpStatusCode.SwitchingProtocols, "HTTP/1.1"));
        }
        /// <summary>The parsed HTTP upgrade request.</summary>
        public HttpRequest Request => _request;
        public HttpResponse? Response => _response;
        /// <summary>
        /// Completes the WebSocket handshake: computes the accept key,
        /// sends HTTP 101 Switching Protocols, and creates a <see cref="WebSocketClient"/>.
        /// </summary>
        public WebSocketClient HttpUpgrade()
        {
            if (_webSocketClient != null)
                return _webSocketClient;
            if(_response != null)
            {
                _response.Headers.Add("Upgrade", "websocket");
                _response.Headers.Add("Connection", "Upgrade");
                // Compute Sec-WebSocket-Accept per RFC 6455 Section 4.2.2
                string secWebSocketKey = Request.Headers.GetValues("Sec-WebSocket-Key").First();
                var sha1 = Convert.ToBase64String(
                    SHA1.HashData(
                        System.Text.Encoding.UTF8.GetBytes(
                            secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
                _response.Headers.Add("Sec-WebSocket-Accept", sha1);
                _response.WriteToStream(_tcpClient.GetStream());
                _webSocketClient = new WebSocketClient(this);
                return _webSocketClient;
            }
            else
            {
                _request.Headers.Add("Connection", "Upgrade");
                _request.Headers.Add("Upgrade", "websocket");
                _request.Headers.Add("Sec-WebSocket-Version", "13");
                _request.Headers.Add("Sec-WebSocket-Key", Convert.ToBase64String(Guid.NewGuid().ToByteArray()));

                _request.WriteToStream(_tcpClient.GetStream());
                _response = HttpResponse.GetRequestFromStream(_tcpClient.GetStream());
                if(_response.StatusCode != HttpStatusCode.SwitchingProtocols)
                {
                    throw new Exception($"HttpStatusCode Not Supported : {_response.StatusCode}");
                }
                if(!_response.Headers.GetValues("Upgrade").Contains("websocket"))
                {
                    throw new Exception($"Upgrade Not Supported : {String.Join(',', _response.Headers.GetValues("Upgrade"))}");
                }
                var secWebSocketAccept = _response.Headers.GetValues("Sec-WebSocket-Accept").First();
                if(secWebSocketAccept != Convert.ToBase64String(SHA1.HashData(System.Text.Encoding.UTF8.GetBytes(_request.Headers.GetValues("Sec-WebSocket-Key").First() + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))))
                {
                    throw new Exception("The Sec-WebSocket-Accept has Error!");
                }
                _webSocketClient = new WebSocketClient(this);
                return _webSocketClient;
            }
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
