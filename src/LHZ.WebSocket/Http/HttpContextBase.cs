using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Holds the HTTP handshake state for one connection and implements both the server-side
    /// and the client-side halves of the WebSocket upgrade.
    /// </summary>
    public class HttpContextBase : IHttpContext
    {
        // Null only between construction and Init, which parses the request from the stream
        // when the caller did not supply one (server role).
        private HttpRequest _request = null!;
        private HttpResponse? _response;
        private Stream _stream;
        private int _status = (int)HttpContextStatus.NotInitialized;
        private WebSocketClient? _webSocketClient;
        private CancellationTokenSource? _timeOutCts;
        /// <summary>The upgraded WebSocket client (null before <see cref="HttpUpgrade"/> is called).</summary>
        public WebSocketClient WebSocketClient => _webSocketClient!;
        /// <summary>The underlying connection stream.</summary>
        public Stream Stream => _stream;
        /// <summary>
        /// Moves the context to Initialized, arms the handshake timeout, and parses the
        /// request from the stream when the caller did not supply one.
        /// </summary>
        /// <param name="timeOut">Handshake timeout in seconds; 0 or less disables it.</param>
        protected void Init(int timeOut)
        {
            _status = (int)HttpContextStatus.Initialized;
            if (timeOut > 0)
            {
                // The timer is cancelled as soon as the handshake leaves the Initialized state,
                // so a completed connection does not keep a pending delay alive.
                _timeOutCts = new CancellationTokenSource();
                var token = _timeOutCts.Token;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(timeOut), token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    // Only the thread that wins this transition may dispose the stream, so the
                    // timeout can never race a successful upgrade that is mid-write.
                    if (TryTransition(HttpContextStatus.Initialized, HttpContextStatus.TimedOut))
                    {
                        try
                        {
                            _stream.Dispose();
                        }
                        catch (Exception)
                        {
                            // The stream is already gone; nothing to do.
                        }
                    }
                });
            }
            if (_request == null)
            {
                _request = HttpRequest.GetRequestFromStream(_stream);
            }
        }

        /// <summary>
        /// Atomically moves the context from one status to another.
        /// Returns false when the context was in some other state.
        /// </summary>
        private bool TryTransition(HttpContextStatus from, HttpContextStatus to)
        {
            return Interlocked.CompareExchange(ref _status, (int)to, (int)from) == (int)from;
        }

        /// <summary>Stops the handshake timeout timer, if one is running.</summary>
        private void CancelTimeOut()
        {
            var cts = Interlocked.Exchange(ref _timeOutCts, null);
            if (cts != null)
            {
                cts.Cancel();
                cts.Dispose();
            }
        }
        /// <summary>Creates a context over the given stream.</summary>
        /// <param name="stream">The connection stream.</param>
        /// <param name="request">The outgoing request (client role), or null to parse one from the stream.</param>
        /// <param name="response">The response to send (server role), or null for the client role.</param>
        protected HttpContextBase(Stream stream, HttpRequest? request, HttpResponse? response)
        {
            _stream = stream;
            _request = request!;
            _response = response;
        }
        /// <summary>Parses the HTTP request from the TCP stream and returns a new context.</summary>
        public static HttpContextBase GetHttpContext(Stream stream, HttpRequest request, int timeOut)
        {
            var context = new HttpContextBase(stream, request, new HttpResponse(HttpStatusCode.SwitchingProtocols, "HTTP/1.1"));
            context.Init(timeOut);
            return context;
        }
        /// <summary>The parsed HTTP upgrade request.</summary>
        public HttpRequest Request => _request;
        /// <summary>
        /// Http Response Info
        /// </summary>
        public HttpResponse? Response => _response;

        /// <summary>The current handshake state.</summary>
        public HttpContextStatus Status => (HttpContextStatus)_status;

        /// <summary>
        /// Completes the WebSocket handshake: computes the accept key,
        /// sends HTTP 101 Switching Protocols, and creates a <see cref="WebSocketClient"/>.
        /// </summary>
        public WebSocketClient HttpUpgrade(int capacity = 1024)
        {
            if (_webSocketClient != null)
                return _webSocketClient;
            // Claim the handshake atomically and stop the timeout timer, so the timer cannot
            // dispose the stream while the response below is being written.
            if (!TryTransition(HttpContextStatus.Initialized, HttpContextStatus.Upgrading))
            {
                if (Status == HttpContextStatus.TimedOut)
                {
                    throw new TimeoutException($"HttpContext Connect Time Out!");
                }
                throw new InvalidOperationException($"Current Status is not allow Upgrade Operation");
            }
            CancelTimeOut();
            if (_response != null)
            {
                // Validate the upgrade request (RFC 6455 §4.2.1): Sec-WebSocket-Key must be
                // present and Sec-WebSocket-Version must be 13. Reject with 400 otherwise.
                if (!Request.Headers.TryGetValues("Sec-WebSocket-Key", out var secWebSocketKeys) ||
                    string.IsNullOrEmpty(secWebSocketKeys.FirstOrDefault()) ||
                    !Request.Headers.TryGetValues("Sec-WebSocket-Version", out var versionValues) ||
                    !versionValues.Contains("13"))
                {
                    var badResponse = new HttpResponse(HttpStatusCode.BadRequest, "HTTP/1.1");
                    badResponse.Headers.Add("Sec-WebSocket-Version", "13");
                    try
                    {
                        badResponse.WriteToStream(_stream);
                    }
                    catch (Exception)
                    {
                        // Client already gone; nothing more to do.
                    }
                    _status = (int)HttpContextStatus.Rejected;
                    throw new InvalidOperationException("Invalid WebSocket upgrade request: missing Sec-WebSocket-Key or Sec-WebSocket-Version != 13");
                }
                _response.Headers.Add("Upgrade", "websocket");
                _response.Headers.Add("Connection", "Upgrade");
                // Compute Sec-WebSocket-Accept per RFC 6455 Section 4.2.2
                string secWebSocketKey = secWebSocketKeys.First();
                var sha1 = Convert.ToBase64String(
                    SHA1.HashData(
                        System.Text.Encoding.UTF8.GetBytes(
                            secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
                _response.Headers.Add("Sec-WebSocket-Accept", sha1);
                _response.WriteToStream(_stream);
                _status = (int)HttpContextStatus.Upgraded;
                _webSocketClient = new WebSocketClient(this, capacity);
                return _webSocketClient;
            }
            else
            {
                _request.Headers.TryAddWithoutValidation("Connection", "Upgrade");
                _request.Headers.TryAddWithoutValidation("Upgrade", "websocket");
                _request.Headers.TryAddWithoutValidation("Sec-WebSocket-Version", "13");
                // RFC 6455 §4.1: the key must be 16 freshly generated random bytes. Guid.NewGuid
                // is not a random source, so draw from the cryptographic RNG instead.
                var nonce = new byte[16];
#if NET6_0_OR_GREATER
                RandomNumberGenerator.Fill(nonce);
#else
                using (var rng = RandomNumberGenerator.Create())
                {
                    rng.GetBytes(nonce);
                }
#endif
                string clientKey = Convert.ToBase64String(nonce);
                _request.Headers.TryAddWithoutValidation("Sec-WebSocket-Key", clientKey);

                _request.WriteToStream(_stream);
                _response = HttpResponse.GetRequestFromStream(_stream);
                if (_response.StatusCode != HttpStatusCode.SwitchingProtocols)
                {
                    throw new Exception($"HttpStatusCode Not Supported : {_response.StatusCode}");
                }
                if (!_response.Headers.TryGetValues("Upgrade", out var upgradeValues) ||
                    !upgradeValues.Contains("websocket", StringComparer.OrdinalIgnoreCase))
                {
                    throw new Exception($"Upgrade Not Supported : {String.Join(",", upgradeValues ?? Enumerable.Empty<string>())}");
                }
                if (!_response.Headers.TryGetValues("Sec-WebSocket-Accept", out var acceptValues) ||
                    acceptValues.FirstOrDefault() == null)
                {
                    throw new Exception("The response is missing the Sec-WebSocket-Accept header!");
                }
                var secWebSocketAccept = acceptValues.First();
                if (secWebSocketAccept != Convert.ToBase64String(SHA1.HashData(System.Text.Encoding.UTF8.GetBytes(clientKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))))
                {
                    throw new Exception("The Sec-WebSocket-Accept has Error!");
                }
                _status = (int)HttpContextStatus.Upgraded;
                _webSocketClient = new WebSocketClient(this, capacity, true);
                return _webSocketClient;
            }
        }

        /// <summary>
        /// Disposes the underlying TCP client if no WebSocket upgrade was performed.
        /// </summary>
        public void Dispose()
        {
            CancelTimeOut();
            // Only tear the stream down when no upgrade took ownership of it. An upgrade that
            // was claimed but then threw (status Upgrading) also has no owner, so it is
            // cleaned up here too.
            if (TryTransition(HttpContextStatus.Initialized, HttpContextStatus.Rejected) ||
                (_webSocketClient == null && TryTransition(HttpContextStatus.Upgrading, HttpContextStatus.Rejected)))
            {
                try
                {
                    _stream.Dispose();
                }
                catch (Exception)
                {
                    // The stream is already gone; nothing to do.
                }
            }
        }
    }
}