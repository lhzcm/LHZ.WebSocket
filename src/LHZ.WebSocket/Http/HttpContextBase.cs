using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading.Tasks;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket.Http
{
    public class HttpContextBase : IHttpContext
    {
        private HttpRequest _request;
        private HttpResponse? _response;
        private Stream _stream;
        private HttpContextStatus _status = HttpContextStatus.NotInitialized;
        private WebSocketClient _webSocketClient = null!;
        private Task? _timeOutExecuter = null;
        /// <summary>The upgraded WebSocket client (null before <see cref="HttpUpgrade"/> is called).</summary>
        public WebSocketClient WebSocketClient => _webSocketClient;
        public Stream Stream => _stream;
        protected void Init(int timeOut)
        {
            _status = HttpContextStatus.Initialized;
            if (timeOut > 0)
            {
                _timeOutExecuter = Task.Run(async () =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(timeOut));
                    if (_status == HttpContextStatus.Initialized)
                    {
                        _status = HttpContextStatus.TimedOut;
                        _stream.Dispose();
                    }
                });
            }
            if (_request == null)
            {
                _request = HttpRequest.GetRequestFromStream(_stream);
            }
        }
        protected HttpContextBase(Stream stream, HttpRequest request, HttpResponse? response)
        {
            _stream = stream;
            _request = request;
            _response = response;
        }
        /// <summary>Parses the HTTP request from the TCP stream and returns a new context.</summary>
        public static HttpContextBase GetHttpContext(Stream stream, HttpRequest request, int timeOut)
        {
            var context = new HttpContextBase(stream, request, new HttpResponse(HttpStatusCode.SwitchingProtocols, "HTTP/1.1"));
            context.Init(timeOut);
            return context;
        }
        // public static HttpContextBase GetHttpContext(Stream stream, int timeOut)
        // {
        //     var context = new HttpContextBase(stream, null, new HttpResponse(HttpStatusCode.SwitchingProtocols, "HTTP/1.1"));
        //     context.Init(timeOut);
        //     return context;
        // }
        /// <summary>The parsed HTTP upgrade request.</summary>
        public HttpRequest Request => _request;
        /// <summary>
        /// Http Response Info
        /// </summary>
        public HttpResponse? Response => _response;

        public HttpContextStatus Status => _status;

        /// <summary>
        /// Completes the WebSocket handshake: computes the accept key,
        /// sends HTTP 101 Switching Protocols, and creates a <see cref="WebSocketClient"/>.
        /// </summary>
        public WebSocketClient HttpUpgrade(int capacity = 1024)
        {
            if (_webSocketClient != null)
                return _webSocketClient;
            if (_status == HttpContextStatus.TimedOut)
            {
                throw new TimeoutException($"HttpContext Connect Time Out!");
            }
            else if (_status != HttpContextStatus.Initialized)
            {
                throw new InvalidOperationException($"Current Status is not allow Upgrade Operation");
            }
            if (_response != null)
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
                _response.WriteToStream(_stream);
                _status = HttpContextStatus.Upgraded;
                _webSocketClient = new WebSocketClient(this, capacity);
                return _webSocketClient;
            }
            else
            {
                _request.Headers.Add("Connection", "Upgrade");
                _request.Headers.Add("Upgrade", "websocket");
                _request.Headers.Add("Sec-WebSocket-Version", "13");
                _request.Headers.Add("Sec-WebSocket-Key", Convert.ToBase64String(Guid.NewGuid().ToByteArray()));

                _request.WriteToStream(_stream);
                _response = HttpResponse.GetRequestFromStream(_stream);
                if (_response.StatusCode != HttpStatusCode.SwitchingProtocols)
                {
                    throw new Exception($"HttpStatusCode Not Supported : {_response.StatusCode}");
                }
                if (!_response.Headers.GetValues("Upgrade").Contains("websocket"))
                {
                    throw new Exception($"Upgrade Not Supported : {String.Join(',', _response.Headers.GetValues("Upgrade"))}");
                }
                var secWebSocketAccept = _response.Headers.GetValues("Sec-WebSocket-Accept").First();
                if (secWebSocketAccept != Convert.ToBase64String(SHA1.HashData(System.Text.Encoding.UTF8.GetBytes(_request.Headers.GetValues("Sec-WebSocket-Key").First() + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))))
                {
                    throw new Exception("The Sec-WebSocket-Accept has Error!");
                }
                _status = HttpContextStatus.Upgraded;
                _webSocketClient = new WebSocketClient(this, capacity);
                return _webSocketClient;
            }
        }

        /// <summary>
        /// Disposes the underlying TCP client if no WebSocket upgrade was performed.
        /// </summary>
        public void Dispose()
        {
            if (_status == HttpContextStatus.Initialized)
            {
                _status = HttpContextStatus.Rejected;
                _stream.Dispose();
            }
        }
    }
}