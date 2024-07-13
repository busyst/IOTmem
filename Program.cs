using System.Net;
using System.Security.Cryptography;
using System.Text;
struct BlockData
{
    public BlockData(uint offset, uint size, DateTime timestamp) : this()
    {
        this.offset = offset;
        this.size = size;
        this.timestamp = timestamp;
    }
    public uint offset;
    public uint size;   
    public DateTime timestamp;   
}
class Program
{
    private const string BaseUrl = "http://localhost:8080/";
    private const uint BlockSize = 256 * 1024; // 256 KiB
    private const string KeyBlockDataFile = "keyblockdata.bin";
    private const string DbName = "main.db";
    private static readonly Dictionary<string, BlockData> KeyBlockData = new(StringComparer.Ordinal);
    private static readonly byte[] IndexPage = File.ReadAllBytes("index.html");

    static async Task Main()
    {
        InitializeDatabase();
        LoadKeyBlockData();

        using var listener = new HttpListener();
        listener.Prefixes.Add(BaseUrl);
        listener.Start();

        Console.WriteLine($"Server started. Listening on {BaseUrl}");

        while (true)
        {
            try
            {
                var context = await listener.GetContextAsync();
                _ = ProcessRequestAsync(context);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing request: {ex.Message}");
            }
        }
    }
    private static void InitializeDatabase()
    {
        if (!File.Exists(DbName))
        {
            using var baseFile = File.Create(DbName, 1024, FileOptions.WriteThrough | FileOptions.RandomAccess);
            Console.WriteLine("Main database created!");
        }
        else
        {
            Console.WriteLine("Main database opened.");
        }
    }

    private static void LoadKeyBlockData()
    {
        if (!File.Exists(KeyBlockDataFile))
        {
            return;
        }

        using var f = File.OpenRead(KeyBlockDataFile);
        using var reader = new BinaryReader(f);

        int size = reader.ReadInt32();
        for (int i = 0; i < size; i++)
        {
            string key = Encoding.ASCII.GetString(reader.ReadBytes(44));
            uint offset = reader.ReadUInt32();
            uint len = reader.ReadUInt32();
            long timestamp = reader.ReadInt64();
            KeyBlockData[key] = new BlockData(offset, len, new DateTime(timestamp));
        }
    }

    private static void SaveKeyBlockData()
    {
        using var f = File.Create(KeyBlockDataFile);
        using var writer = new BinaryWriter(f);

        writer.Write(KeyBlockData.Count);
        foreach (var (key, value) in KeyBlockData)
        {
            writer.Write(Encoding.ASCII.GetBytes(key));
            writer.Write(value.offset);
            writer.Write(value.size);
            writer.Write(value.timestamp.Ticks);
        }
    }

    private static async Task ProcessRequestAsync(HttpListenerContext context)
    {
        var request = context.Request;
        var response = context.Response;

        try
        {
            var urlString = request.Url?.ToString() ?? throw new ArgumentNullException(nameof(request.Url));

            switch (urlString)
            {
                case BaseUrl when request.HttpMethod == "GET":
                    await SendResponseAsync(response, IndexPage, "text/html");
                    break;

                case var s when s.StartsWith(BaseUrl + "key") && request.HttpMethod == "GET":
                    await HandleGetApiKeyAsync(response);
                    break;

                case var s when s.StartsWith(BaseUrl + "api/get/") && request.HttpMethod == "GET":
                    await HandleApiRequestGetAsync(response, urlString[(BaseUrl + "api/get/").Length..]);
                    break;
                case var s when s.StartsWith(BaseUrl + "api/set/") && request.HttpMethod == "POST":
                    await HandleApiRequestSetAsync(request,response, urlString[(BaseUrl + "api/set/").Length..]);
                    break;
                default:
                    System.Console.WriteLine(urlString+" not found!");
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error handling request: {ex.Message}");
            response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
        finally
        {
            response.Close();
        }
    }

    private static async Task HandleGetApiKeyAsync(HttpListenerResponse response)
    {
        var key = GenerateUniqueAPIKey();
        uint offset = CalculateOffset();
        KeyBlockData[Convert.ToBase64String(key.Data)] = new BlockData(offset, BlockSize, DateTime.UtcNow);
        SaveKeyBlockData(); // Save after adding new key

        await SendResponseAsync(response, Encoding.ASCII.GetBytes(Convert.ToBase64String(key.Data)), "text/plain");
    }

    private static async Task HandleApiRequestGetAsync(HttpListenerResponse response, string uri)
    {
        Console.WriteLine($"API request received with key: {uri}");
        if (uri.Length < 44)
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "API key too short");
            return;
        }

        string key = uri[..44];
        if (!KeyBlockData.TryGetValue(key, out var blockData))
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Key is invalid");
            return;
        }

        string[] data = uri[44..].Split('+', StringSplitOptions.RemoveEmptyEntries);
        if (data.Length != 2 || !uint.TryParse(data[0], out var offset) || !uint.TryParse(data[1], out var len))
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Offset and/or length not parsable");
            return;
        }

        uint endIndex = offset + len;
        if (endIndex > blockData.size)
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Index out of bounds");
            return;
        }

        byte[] buffer = new byte[len];
        using var baseFile = File.Open(DbName, FileMode.Open, FileAccess.Read);
        baseFile.Seek(offset + blockData.offset, SeekOrigin.Begin);
        baseFile.Read(buffer);
        await SendResponseAsync(response, buffer, "application/octet-stream");
    }
    private static async Task HandleApiRequestSetAsync(HttpListenerRequest request, HttpListenerResponse response, string uri)
    {
        Console.WriteLine($"API request received with key: {uri}");
        if (uri.Length < 44)
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "API key too short");
            return;
        }

        string key = uri[..44];
        if (!KeyBlockData.TryGetValue(key, out var blockData))
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Key is invalid");
            return;
        }

        string[] data = uri[44..].Split('+', StringSplitOptions.RemoveEmptyEntries);
        if (data.Length != 2 || !uint.TryParse(data[0], out var offset) || !uint.TryParse(data[1], out var len))
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Offset and/or length not parsable");
            return;
        }

        uint endIndex = offset + len;
        if (endIndex > blockData.size)
        {
            await SendErrorResponseAsync(response, HttpStatusCode.BadRequest, "Index out of bounds");
            return;
        }

        byte[] requestData = new byte[request.ContentLength64];
        await request.InputStream.ReadAsync(requestData);
        using var baseFile = File.Open(DbName, FileMode.Open, FileAccess.Write);
        baseFile.Seek(offset + blockData.offset, SeekOrigin.Begin);
        await baseFile.WriteAsync(requestData);
        await SendResponseAsync(response, Array.Empty<byte>(), "");
    }
    private static async Task SendResponseAsync(HttpListenerResponse response, byte[] content, string contentType)
    {
        response.ContentType = contentType;
        response.ContentLength64 = content.Length;
        await response.OutputStream.WriteAsync(content);
    }
    private static Task SendErrorResponseAsync(HttpListenerResponse response, HttpStatusCode statusCode, string message)
    {
        response.StatusCode = (int)statusCode;
        return SendResponseAsync(response, Encoding.UTF8.GetBytes(message), "text/plain");
    }

    private static APIKey GenerateUniqueAPIKey()
    {
        APIKey key;
        string base64Key;
        do
        {
            key = GenerateRandomAPIKey();
            base64Key = Convert.ToBase64String(key.Data);
        } while (KeyBlockData.ContainsKey(base64Key));
        return key;
    }

    private static uint CalculateOffset()
    {
        uint offset = 0;
        foreach (var kvp in KeyBlockData)
        {
            var d = kvp.Value;
            if (offset >= d.offset && offset < (d.offset + d.size))
            {
                offset = d.offset + d.size;
            }
        }
        return offset;
    }

    private static APIKey GenerateRandomAPIKey()
    {
        using var rng = RandomNumberGenerator.Create();
        byte[] randomBytes = new byte[32];
        rng.GetBytes(randomBytes);
        return new APIKey(randomBytes);
    }
}

readonly struct APIKey : IEquatable<APIKey>
{
    public readonly byte[] Data;

    public APIKey(byte[] data) => Data = data;

    public bool Equals(APIKey other) => Data.AsSpan().SequenceEqual(other.Data);
    public override bool Equals(object? obj) => obj is APIKey key && Equals(key);
    public override int GetHashCode() => BitConverter.ToInt32(Data, 0);
}