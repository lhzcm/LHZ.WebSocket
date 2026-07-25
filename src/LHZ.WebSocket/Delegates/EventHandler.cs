namespace LHZ.WebSocket.Delegates
{
    public delegate void EventHandler<in TSender, TEventArgs>(TSender sender,  TEventArgs e);
}
