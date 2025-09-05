using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace PbDatabase;

//TODO добавить алгоритм вытеснения
public sealed class PageManager
{
    internal const int PageSize = 8 * 1024;
    private const int HeadersSize = 0;

    private SpinLock _lock;

    private readonly LoadedPage[] _array;
    private int _arraySize;

    private readonly Dictionary<long, LoadedPage> _chunks;
    private readonly FileStream _fileStream;
    private readonly object _fileLock;

    public PageManager(FileStream fileStream)
    {
        _fileStream = fileStream;
        _lock = new SpinLock();
        _fileLock = new object();
        _chunks = new Dictionary<long, LoadedPage>();
        _array = new LoadedPage[CalculateCapacity(fileStream)];
    }

    internal int ReadFromMemory(int offset, Span<LoadedPage> buffer)
    {
        bool locked = false;
        try
        {
            _lock.Enter(ref locked);
            Debug.Assert(locked);

            var length = _arraySize - offset;

            if (length <= 0)
                return 0;

            length = Math.Max(length, buffer.Length);
            _array.AsSpan(offset, length).CopyTo(buffer);

            foreach (var loadedPage in buffer)
            {
                loadedPage.Pin();
            }

            return length;
        }
        finally
        {
            if (locked)
                _lock.Exit();
        }
    }

    public LoadedPage GetAndPin(long pageNumber)
    {
        bool locked = false;
        try
        {
            _lock.Enter(ref locked);
            Debug.Assert(locked);

            if (_chunks.TryGetValue(pageNumber, out var result))
            {
                result.Pin();
                return result;
            }
        }
        finally
        {
            if (locked)
                _lock.Exit();
        }

        return LoadAndPin(pageNumber);
    }

    private LoadedPage LoadAndPin(long pageNumber)
    {
        var loaded = ReadFromDisk(pageNumber);

        bool locked = false;
        try
        {
            _lock.Enter(ref locked);

            if (_chunks.TryGetValue(pageNumber, out var result))
            {
                result.Pin();
                return result;
            }

            _chunks.Add(pageNumber, loaded);
            _array[_arraySize++] = loaded;
            loaded.Pin();
            return loaded;
        }
        finally
        {
            if (locked)
                _lock.Exit();
        }
    }

    private LoadedPage ReadFromDisk(long pageNumber)
    {
        var buffer = new byte[PageSize]; //TODO pool

        var offset = GetPageOffset(pageNumber);

        lock (_fileLock)
        {
            _fileStream.Seek(offset, SeekOrigin.Begin);

            _fileStream.ReadExactly(buffer, 0, PageSize);
        }

        var page = new PageBuffer(buffer);

        if (!page.IsCheckSumValid)
        {
            throw new InvalidOperationException("Page is in invalid state due to checksum");
        }

        if (page.Number != pageNumber)
        {
            throw new InvalidOperationException("Page is in invalid state doe to incorrect number");
        }

        return new LoadedPage(page);
    }

    internal void DumpPage(LoadedPage page)
    {
        var offset = GetPageOffset(page.PageBuffer.Number);
        page.PageBuffer.RecomputeCheckSum();

        lock (_fileLock)
        {
            _fileStream.Seek(offset, SeekOrigin.Begin);
            _fileStream.Write(page.PageBuffer.RawBuffer);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetPageOffset(long pageNumber)
    {
        return HeadersSize + pageNumber * PageSize;
    }

    private static int CalculateCapacity(FileStream fileStream)
    {
        throw new NotImplementedException();
    }

    public void FlushBuffers()
    {
        lock (_fileLock)
        {
            _fileStream.Flush(true);
        }
    }
}