namespace LHZ.WebSocket.Delegates
{
    /// <summary>
    /// A contravariant event handler, so a handler written against an interface can be
    /// attached to an event declared on a concrete sender type.
    /// </summary>
    /// <typeparam name="TSender">The type raising the event.</typeparam>
    /// <typeparam name="TEventArgs">The payload passed to the handler.</typeparam>
    /// <param name="sender">The object raising the event.</param>
    /// <param name="e">The event payload.</param>
    public delegate void EventHandler<in TSender, TEventArgs>(TSender sender,  TEventArgs e);
}
