namespace LHZ.WebSocket.Server.Enums;

/// <summary>
/// Lifecycle states of the WebSocket server.
/// </summary>
public enum ServerStatus : byte
{
    /// <summary>Initialized but not yet started.</summary>
    Ready = 0,

    /// <summary>Actively listening for connections.</summary>
    Start = 1,

    /// <summary>Shutting down — disconnecting clients.</summary>
    Closing = 2,

    /// <summary>Fully stopped.</summary>
    Closed = 3
}
