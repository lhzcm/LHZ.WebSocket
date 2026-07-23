namespace LHZ.WebSocket.Server.Enums;

/// <summary>
/// Lifecycle states of a WebSocket client connection.
/// </summary>
public enum ClientStatus : byte
{
    /// <summary>TCP connected but WebSocket handshake not yet completed.</summary>
    Connection = 0,

    /// <summary>Handshake complete, ready to send and receive frames.</summary>
    Opend = 1,

    /// <summary>Connection closed (locally or by peer).</summary>
    Close = 2
}
