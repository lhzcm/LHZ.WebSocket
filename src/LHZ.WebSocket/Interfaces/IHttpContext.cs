using System;
using System.IO;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Http;

namespace LHZ.WebSocket.Interfaces
{
        /// <summary>
        ///Wraps an incoming HTTP upgrade request and provides the WebSocket handshake logic.
        /// Created per-connection by <see cref="WebSocketServer"/> and disposed after use.
        /// </summary>
        public interface IHttpContext : IDisposable
        {
                /// <summary>The parsed HTTP upgrade request.</summary>
                public HttpRequest Request { get; }
                /// <summary>
                /// Http Response Info
                /// </summary>
                public HttpResponse Response { get; }
                /// <summary>
                /// Completes the WebSocket handshake: computes the accept key,
                /// sends HTTP 101 Switching Protocols, and creates a <see cref="WebSocketClient"/>.
                /// </summary>
                public WebSocketClient HttpUpgrade(int capacity = 1024);
                /// <summary>
                /// The status of the HTTP context.
                /// </summary>
                public HttpContextStatus Status { get; }
                /// <summary>
                /// The stream associated with the HTTP context.
                /// </summary>
                public Stream Stream { get; }
        }
}
