// See https://aka.ms/new-console-template for more information

using System.Security.Cryptography;
using PbDatabase;

var stream = File.Open("test.pbdb", FileMode.Create, FileAccess.ReadWrite, FileShare.Read);

stream.SetLength(8 * 1024 * 16);

stream.Flush(true);

var loader = new PageManager(stream);

var page = loader.GetAndPin(0);

bool locked = false;
try
{
    page.LockWrite(ref locked);

    RandomNumberGenerator.Fill(page.PageBuffer.Data);
}
finally
{
    if (locked)
        page.ReleaseLock();
    page.Unpin();
}