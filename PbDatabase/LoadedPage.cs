using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace PbDatabase;

public sealed class LoadedPage
{
    internal const int Dirty = 0b00000001;
    internal const int IoInProgress = 0b00000010;

    public readonly PageBuffer PageBuffer;
    internal readonly ManualResetEventSlim EventSlim;

    private SpinLock _lock;
    private int _counter;
    internal volatile int Flags;

    public bool IsPinned => Interlocked.CompareExchange(ref _counter, 0, 0) != 0;

    public bool IsPinnedMultipleTimes => Interlocked.CompareExchange(ref _counter, 0, 0) > 1;

    public LoadedPage(PageBuffer pageBuffer)
    {
        PageBuffer = pageBuffer;
        EventSlim = new ManualResetEventSlim();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void LockRead(ref bool success)
    {
        _lock.Enter(ref success);
        Debug.Assert(success);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void LockWrite(ref bool success)
    {
        while (true)
        {
            _lock.Enter(ref success);
            Debug.Assert(success);

            if ((Flags & IoInProgress) == 0)
                return;

            if (success)
                _lock.Exit();

            EventSlim.Wait();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseLock()
    {
        _lock.Exit();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Pin()
    {
        Interlocked.Increment(ref _counter);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Unpin()
    {
        var value = Interlocked.Decrement(ref _counter);

        Debug.Assert(value >= 0);
    }
}