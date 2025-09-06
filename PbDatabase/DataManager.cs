using System.Runtime.CompilerServices;

namespace PbDatabase;

public sealed class DataManager
{
    private const int DataPerPage = PageBuffer.PayloadLength;
    private readonly PageManager _pageManager;

    public DataManager(PageManager pageManager)
    {
        _pageManager = pageManager;
    }

    public void Read(long offset, Span<byte> destination)
    {
        var startPage = GetPageByOffset(offset);
        var endPage = GetPageByOffset(offset + destination.Length);
        for (long i = startPage; i < endPage; i++)
        {
            var copyToBuffer = destination.Slice((int)(i - startPage) * DataPerPage);
            var page = _pageManager.GetAndPin(i);
            bool locked = false;
            try
            {
                page.LockRead(ref locked);

                page.PageBuffer.Payload.CopyTo(copyToBuffer);
            }
            finally
            {
                if (locked)
                    page.ReleaseLock();

                page.Unpin();
            }
        }

        var endCopyToBuffer = destination.Slice((int)(endPage - startPage) * DataPerPage);
        var lastPage = _pageManager.GetAndPin(endPage);
        var lockedSuccess = false;
        try
        {
            lastPage.LockRead(ref lockedSuccess);

            lastPage.PageBuffer.Payload.Slice(0, endCopyToBuffer.Length).CopyTo(endCopyToBuffer);
        }
        finally
        {
            if (lockedSuccess)
                lastPage.ReleaseLock();

            lastPage.Unpin();
        }
    }

    public void Write(long offset, ReadOnlySpan<byte> source)
    {
        var startPage = GetPageByOffset(offset);
        var endPage = GetPageByOffset(offset + source.Length);
        for (long i = startPage; i <= endPage; i++)
        {
            var sourceOffset = (int)(i - startPage) * DataPerPage;
            var sourceChunk = source.Slice(sourceOffset, Math.Min(DataPerPage, source.Length - sourceOffset));
            var page = _pageManager.GetAndPin(i);
            bool locked = false;
            try
            {
                page.LockWrite(ref locked);
                sourceChunk.CopyTo(page.PageBuffer.Payload);
                Interlocked.Or(ref page.Flags, LoadedPage.Dirty);
            }
            finally
            {
                if (locked)
                    page.ReleaseLock();

                page.Unpin();
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetPageByOffset(long offset)
    {
        return (long)Math.Floor(offset / (double)DataPerPage);
    }
}