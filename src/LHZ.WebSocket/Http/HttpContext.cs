
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using LHZ.WebSocket.Enums;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Wraps an incoming HTTP upgrade request and provides the WebSocket handshake logic.
    /// Created per-connection by <see cref="WebSocketServer"/> and disposed after use.
    /// </summary>
    public sealed class HttpContext : HttpContextBase, IHttpContext, IDisposable
    {
        private TcpClient _tcpClient;
        /// <summary>The TCP connection this context was built from.</summary>
        public TcpClient TcpClient => _tcpClient;
        private HttpContext(TcpClient tcpClient, HttpRequest? request, HttpResponse? response) : base(tcpClient.GetStream(), request, response)
        {
            _tcpClient = tcpClient;
        }
        /// <summary>Wraps an outgoing request (client role) and returns a new context.</summary>
        internal static HttpContext GetHttpContext(TcpClient tcpClient, HttpRequest request, int timeOut)
        {
            var context = new HttpContext(tcpClient, request, null);
            context.Init(timeOut);
            return context;
        }
        /// <summary>Parses the HTTP request from the TCP stream (server role) and returns a new context.</summary>
        internal static HttpContext GetHttpContext(TcpClient tcpClient, int timeOut)
        {
            var context = new HttpContext(tcpClient, null, new HttpResponse(HttpStatusCode.SwitchingProtocols, "HTTP/1.1"));
            context.Init(timeOut);
            return context;
        }
    }
}
