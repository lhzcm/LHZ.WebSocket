using LHZ.WebSocket.Server;
using LHZ.WebSocket.Server.Core;
using LHZ.WebSocket.Server.Http;

// Create a WebSocket server on port 5000
WebSocketServer webSocketServer = new WebSocketServer(5000);

// Handle each incoming HTTP upgrade request
webSocketServer.OnUpgradeRequest += (HttpContext headers) =>
{
    headers.ResponseHeaders.Add("test", "true");
    var client = headers.HttpUpgrade();

    // Echo text messages back to the client
    client.OnMessageReceived += (WebSocketClient sender, string message) =>
    {
        Console.WriteLine(message);
        client.SendMessage($"server recive : {message}");
    };

    // Handle client-initiated close
    client.OnCloseRecived += (WebSocketClient sender, CloseMessage message) =>
    {
        Console.WriteLine(message);
        sender.Close();
    };
};

webSocketServer.Start();

Console.WriteLine("webSocketServer is Start");
Console.ReadLine();
webSocketServer.Stop();
Console.WriteLine("webSocketServer is Stop");