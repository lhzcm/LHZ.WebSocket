
namespace LHZ.WebSocket.Server.Http;

public sealed class HttpContext
{
    private HttpRequest _request;
    private HttpResponseHeaders _responseHeaders;

    internal HttpContext(HttpRequest request)
    {
        _request = request;
        _responseHeaders = new HttpResponseHeaders();
    }

    public sealed class HttpResponseHeaders
    {
        private readonly List<KeyValuePair<string, string>> _headers = new();

        internal HttpResponseHeaders() { }

        public void Add(string name, string value)
        {
            _headers.Add(new KeyValuePair<string, string>(name, value));
        }

        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            foreach (var header in _headers)
            {
                sb.Append(header.Key);
                sb.Append(": ");
                sb.Append(header.Value);
                sb.Append('\n');
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// The parsed HTTP upgrade request.
    /// </summary>
    public HttpRequest Request => _request;

    /// <summary>
    /// Response headers to send back (e.g., Upgrade, Sec-WebSocket-Accept).
    /// </summary>
    public HttpResponseHeaders ResponseHeaders => _responseHeaders;
}
