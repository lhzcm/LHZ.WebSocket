using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Core
{
    /// <summary>
    /// Represents a WebSocket close frame payload: a status code and optional reason string.
    /// </summary>
    public struct CloseMessage
    {
        /// <summary>Creates a close message.</summary>
        /// <param name="opCode">The close status code.</param>
        /// <param name="message">The human-readable close reason; may be empty.</param>
        public CloseMessage(CloseCode opCode, string message)
        {
            CloseCode = opCode;
            Message = message;
        }

        /// <summary>WebSocket close status code (e.g., 1000 Normal).</summary>
        public CloseCode CloseCode { get; private set; }

        /// <summary>Optional human-readable close reason.</summary>
        public string Message { get; private set; }
    }
}