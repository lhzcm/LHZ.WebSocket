namespace LHZ.WebSocket.Server.Enums;

/// <summary>
/// WebSocket frame opcodes as defined in RFC 6455 Section 5.2.
/// </summary>
public enum OpCode : byte
{
    /// <summary>Continuation frame (continues a fragmented message).</summary>
    Continuation = 0x0,

    /// <summary>UTF-8 text message frame.</summary>
    Text = 0x1,

    /// <summary>Binary data frame.</summary>
    Binary = 0x2,

    /// <summary>Connection close frame.</summary>
    Close = 0x8,

    /// <summary>Ping frame (keep-alive / latency check).</summary>
    Ping = 0x9,

    /// <summary>Pong frame (response to ping).</summary>
    Pong = 0xA
}