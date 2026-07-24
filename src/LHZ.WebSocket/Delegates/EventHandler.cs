namespace LHZ.WebSocket.Delegates
{
    public delegate void EventHandler<TSender, TEventArgs>(in TSender sender,  TEventArgs e);
}
