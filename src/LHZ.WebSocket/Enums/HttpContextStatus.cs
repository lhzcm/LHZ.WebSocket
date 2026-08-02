namespace LHZ.WebSocket.Enums
{
    public enum HttpContextStatus
    {
        NotInitialized = 0,
        Initialized = 1,
        Upgraded = 2,
        TimedOut = -2,
        Rejected = -1
    }
}
