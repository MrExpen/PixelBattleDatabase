// See https://aka.ms/new-console-template for more information

using System.Text;
using PbDatabase;

const long payloadLength = 1024 * 1024;

var stream = File.Open("test.pbdb", FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
PageManager.InitializeFilePayloadSize(stream, payloadLength);

var pageManager = new PageManager(stream);
var backgroundWriter = new BackgroundWriter(pageManager);
var dataManager = new DataManager(pageManager);

Span<byte> helloWorld = "Hello world!"u8.ToArray();

dataManager.Write(0, helloWorld);

Span<byte> buffer = stackalloc byte[helloWorld.Length];

dataManager.Read(0, buffer);

Console.WriteLine(Encoding.UTF8.GetString(buffer));

backgroundWriter.SyncOnce();

var maxPayloadLength = pageManager.GetMaxPayloadLength();

Console.WriteLine(maxPayloadLength >= payloadLength);