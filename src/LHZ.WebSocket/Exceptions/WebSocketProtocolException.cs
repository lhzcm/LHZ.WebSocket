using System;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Exceptions
{
    /// <summary>
    /// Raised when the peer violates the WebSocket protocol (RFC 6455).
    /// Carries the close code that should be sent to the peer before the connection is failed.
    /// </summary>
    public class WebSocketProtocolException : Exception
    {
        /// <summary>The close code to report to the peer.</summary>
        public CloseCode CloseCode { get; }

        /// <summary>
        /// Creates a new protocol exception.
        /// </summary>
        /// <param name="closeCode">The close code to report to the peer.</param>
        /// <param name="message">A description of the violation.</param>
        public WebSocketProtocolException(CloseCode closeCode, string message) : base(message)
        {
            CloseCode = closeCode;
        }

        /// <summary>
        /// Creates a new protocol exception wrapping an inner cause.
        /// </summary>
        /// <param name="closeCode">The close code to report to the peer.</param>
        /// <param name="message">A description of the violation.</param>
        /// <param name="innerException">The underlying cause.</param>
        public WebSocketProtocolException(CloseCode closeCode, string message, Exception? innerException)
            : base(message, innerException)
        {
            CloseCode = closeCode;
        }
    }
}
