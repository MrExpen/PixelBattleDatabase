namespace PbDatabase;

public sealed class BackgroundWriter
{
    private const int BatchSize = 128;

    private readonly PageManager _pageManager;
    private readonly TimeSpan _delay;

    public BackgroundWriter(PageManager pageManager) : this(pageManager, TimeSpan.FromMinutes(5))
    {
    }

    public BackgroundWriter(PageManager pageManager, TimeSpan delay)
    {
        _pageManager = pageManager;
        _delay = delay;
    }

    public async Task LoopAsync(CancellationToken cancellation)
    {
        var buffer = new LoadedPage[BatchSize];
        while (!cancellation.IsCancellationRequested)
        {
            await Task.Delay(_delay, cancellation);

            Sync(buffer);
        }
    }

    private void Sync(Span<LoadedPage> buffer)
    {
        int offset = 0;
        int read = 0;

        while (true)
        {
            try
            {
                read = _pageManager.ReadFromMemory(offset, buffer);

                if (read == 0)
                    break;
                offset += read;

                for (var i = 0; i < read; i++)
                {
                    var page = buffer[i];

                    bool locked = false;
                    try
                    {
                        page.LockRead(ref locked);

                        if ((page.Flags & (LoadedPage.Dirty | LoadedPage.IoInProgress)) != LoadedPage.Dirty)
                            continue;

                        page.EventSlim.Reset();
                        Interlocked.Or(ref page.Flags, LoadedPage.IoInProgress);
                    }
                    finally
                    {
                        if (locked)
                            page.ReleaseLock();
                    }

                    _pageManager.DumpPage(page);

                    locked = false;
                    try
                    {
                        page.LockRead(ref locked);

                        Interlocked.And(ref page.Flags, ~(LoadedPage.Dirty | LoadedPage.IoInProgress));
                    }
                    finally
                    {
                        if (locked)
                            page.ReleaseLock();
                    }

                    page.EventSlim.Set();
                }
            }
            finally
            {
                for (int i = 0; i < read; i++)
                {
                    buffer[i].Unpin();
                }
            }
        }
    }

    internal void SyncOnce()
    {
        Sync(new LoadedPage[BatchSize]);
    }
}