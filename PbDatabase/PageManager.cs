using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace PbDatabase;

public sealed class PageManager
{
    internal const int PageSize = 8 * 1024;
    private const int HeadersSize = 0;

    private SpinLock _lock;
    // TODO: rewrite for own dictionary with optimizations
    private readonly Dictionary<long, LoadedPage> _cache; //TODO preallocate
    private readonly FileStream _fileStream;
    private readonly object _fileLock;

    public PageManager(FileStream fileStream)
    {
        _fileStream = fileStream;
        _lock = new SpinLock();
        _fileLock = new object();
        _cache = [];
    }

    public LoadedPage GetAndPin(long pageNumber)
    {
        bool locked = false;
        try
        {
            _lock.Enter(ref locked);
            Debug.Assert(locked);

            if (_cache.TryGetValue(pageNumber, out var result))
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
        //Load from disk
        var loaded = ReadFromDisk(pageNumber);

        bool locked = false;
        try
        {
            _lock.Enter(ref locked);

            if (_cache.TryGetValue(pageNumber, out var result))
            {
                result.Pin();
                return result;
            }

            _cache.Add(pageNumber, loaded);
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

    private void DumpPageWithLock(LoadedPage page)
    {
        var locked = false;

        try
        {
            page.LockWrite(ref locked);
            Debug.Assert(locked);

            page.IsIoInProgress = true;
        }
        finally
        {
            if (locked)
                page.ReleaseLock();
        }
        
        
    }

    private void DumpPage(LoadedPage page)
    {
        var offset = GetPageOffset(page.PageBuffer.Number);
        page.PageBuffer.RecomputeCheckSum();

        lock (_fileLock)
        {
            _fileStream.Seek(offset, SeekOrigin.Begin);
            _fileStream.Write(page.PageBuffer.RawBuffer);
        }

        page.IsDirty = false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetPageOffset(long pageNumber)
    {
        return HeadersSize + pageNumber * PageSize;
    }
}