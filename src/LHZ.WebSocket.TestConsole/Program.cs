using LHZ.WebSocket.Server;
using LHZ.WebSocket.Server.Http;

WebSocketServer webSocketServer = new WebSocketServer(5000);
webSocketServer.OnUpgradeRequest += (HttpContext headers) =>
{
    return true;
};
webSocketServer.OnClientConnected += (WebSocketClient client) =>
{
    client.OnMessageReceived += (WebSocketClient client, byte[] message)=>
    {
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(message));
    };
    client.StartReceiving();
};

webSocketServer.Start();