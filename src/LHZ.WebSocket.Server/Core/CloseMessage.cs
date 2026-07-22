using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server.Core;

public struct CloseMessage
{
    public CloseMessage(CloseCode opCode, string message)
    {
        CloseCode = opCode;
        Message = message;
    }
    public CloseCode CloseCode {get; private set;}
    public string Message {get; private set;}
}
