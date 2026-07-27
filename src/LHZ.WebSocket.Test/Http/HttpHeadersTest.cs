using System.ComponentModel.DataAnnotations;

namespace LHZ.WebSocket.Test.Http;

public class HttpHeadersTest
{
    [Fact]
    public void AddTest()
    {
        var header = new LHZ.WebSocket.Http.HttpHeaders();
        try
        {
            header.Add("User-Agent", "Mozilla/5.0 (Linux; Android 16; PJZ110 Build/BP2A.250605.015; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/146.0.7680.178 Mobile Safari/537.36 XWEB/1460243 MMWEBSDK/20260502 MMWEBID/1068 REV/379ee0b45c94853caaf778fe44cd28565b749bd1 MicroMessenger/8.0.72.3100(0x28004853) WeChat/arm64 Weixin NetType/WIFI Language/zh_CN ABI/arm64");
        }
        catch(Exception)
        {
            header.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Linux; Android 16; PJZ110 Build/BP2A.250605.015; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/146.0.7680.178 Mobile Safari/537.36 XWEB/1460243 MMWEBSDK/20260502 MMWEBID/1068 REV/379ee0b45c94853caaf778fe44cd28565b749bd1 MicroMessenger/8.0.72.3100(0x28004853) WeChat/arm64 Weixin NetType/WIFI Language/zh_CN ABI/arm64");
        }
    }
}
