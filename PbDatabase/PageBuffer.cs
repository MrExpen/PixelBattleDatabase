using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace PbDatabase;

public sealed class PageBuffer
{
    private const int CheckSumOffset = 0;
    private const int CheckSumSize = 4;
    private const int NumberOffset = CheckSumSize;
    private const int LsnOffset = 12;
    private const int DataOffset = 32;
    private const int DataLength = PageManager.PageSize - DataOffset;

    private readonly byte[] _buffer;

    public PageBuffer(byte[] buffer)
    {
        _buffer = buffer;
    }

    public bool IsCheckSumValid => CheckSum == ComputeCheckSum();

    public uint CheckSum
    {
        get => BinaryPrimitives.ReadUInt32BigEndian(_buffer.AsSpan(CheckSumOffset));
        private set => BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(CheckSumOffset), value);
    }

    public long Number => BinaryPrimitives.ReadInt64BigEndian(_buffer.AsSpan(NumberOffset));

    public long Lsn
    {
        get => BinaryPrimitives.ReadInt64BigEndian(_buffer.AsSpan(LsnOffset));
        set => BinaryPrimitives.WriteInt64BigEndian(_buffer.AsSpan(LsnOffset), value);
    }

    public Span<byte> Data => _buffer.AsSpan(DataOffset, DataLength);

    public Span<byte> RawBuffer => _buffer.AsSpan(0, PageManager.PageSize);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecomputeCheckSum()
    {
        CheckSum = ComputeCheckSum();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint ComputeCheckSum()
    {
        return Crc32.HashToUInt32(_buffer.AsSpan(CheckSumOffset + CheckSumSize));
    }
}