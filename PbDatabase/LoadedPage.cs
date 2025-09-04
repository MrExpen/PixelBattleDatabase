using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace PbDatabase;

public sealed class LoadedPage
{
    private const int Dirty = 0b00000001;
    private const int IoInProgress = 0b00000010;

    public readonly PageBuffer PageBuffer;
    private readonly ManualResetEventSlim _eventSlim;

    private SpinLock _lock;
    private int _counter;
    private int _flags;

    public bool IsDirty
    {
        get => (_flags & Dirty) == Dirty;
        set => _flags = value ? _flags | Dirty : _flags & ~Dirty;
    }

    public bool IsIoInProgress
    {
        get => (_flags & IoInProgress) == IoInProgress;
        set => _flags = value ? _flags | IoInProgress : _flags & ~IoInProgress;
    }

    public bool IsPinned => Interlocked.CompareExchange(ref _counter, 0, 0) != 0;

    public LoadedPage(PageBuffer pageBuffer)
    {
        PageBuffer = pageBuffer;
        _eventSlim = new ManualResetEventSlim();
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

            if (!IsIoInProgress)
                return;

            if (success)
                _lock.Exit();

            _eventSlim.Wait();
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