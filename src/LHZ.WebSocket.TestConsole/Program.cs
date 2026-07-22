using LHZ.WebSocket.Server;
using LHZ.WebSocket.Server.Core;
using LHZ.WebSocket.Server.Http;

WebSocketServer webSocketServer = new WebSocketServer(5000);
webSocketServer.OnUpgradeRequest += (HttpContext headers) =>
{
    return true;
};
webSocketServer.OnClientConnected += (WebSocketClient client) =>
{
    client.OnMessageReceived += (WebSocketClient sender, string message)=>
    {
        Console.WriteLine(message);
        client.SendMessage($"server recive : {message}");
    };
    client.OnCloseRecived += (WebSocketClient sender, CloseMessage message) =>
    {
        Console.WriteLine(message);
        sender.Close();
    };
};

webSocketServer.Start();