namespace LHZ.WebSocket.Enums
{
    /// <summary>
    /// The lifecycle state of an HTTP context during the WebSocket handshake.
    /// </summary>
    public enum HttpContextStatus
    {
        /// <summary>The context has been constructed but the request has not been read yet.</summary>
        NotInitialized = 0,
        /// <summary>The request has been read and the handshake may still be accepted.</summary>
        Initialized = 1,
        /// <summary>The handshake completed and a WebSocket client owns the connection.</summary>
        Upgraded = 2,
        /// <summary>
        /// The handshake has been claimed and is in progress. Reached briefly between
        /// Initialized and Upgraded so the handshake timeout cannot dispose the stream
        /// while the 101 response is being written.
        /// </summary>
        Upgrading = 3,
        /// <summary>The handshake did not complete within the configured timeout.</summary>
        TimedOut = -2,
        /// <summary>The upgrade was refused, or the context was disposed without upgrading.</summary>
        Rejected = -1
    }
}
