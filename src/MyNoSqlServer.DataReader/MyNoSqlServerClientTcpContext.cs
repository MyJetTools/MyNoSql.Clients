using System;
using System.Diagnostics;
using System.Threading.Tasks;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlServerClientTcpContext : ClientTcpContext<IMyNoSqlTcpContract>
    {
        private readonly MyNoSqlSubscriber _subscriber;
        private readonly string _appName;

        public MyNoSqlServerClientTcpContext(MyNoSqlSubscriber subscriber, string appName)
        {
            _subscriber = subscriber;
            _appName = appName;
        }

        
        private static readonly Lazy<string> GetReaderVersion = new Lazy<string>(() =>
        {
            try
            {
                return typeof(MyNoSqlServerClientTcpContext).Assembly.GetName().Version.ToString();
            }
            catch (Exception)
            {
                return null;
            }
        });


        protected override ValueTask OnConnectAsync()
        {

            var readerVersion = GetReaderVersion.Value;

            readerVersion = readerVersion == null ? "" : ";ReaderVersion:" + readerVersion;

            var greetingsContract = new GreetingContract
            {
                Name = _appName + readerVersion
            };

            SendDataToSocket(greetingsContract);

            foreach (var tableToSubscribe in _subscriber.GetTablesToSubscribe())
            {
                var subscribePacket = new SubscribeContract
                {
                    TableName = tableToSubscribe
                };

                SendDataToSocket(subscribePacket);

                Console.WriteLine($"[NoSql][{_appName}]Subscribed to MyNoSql table: {tableToSubscribe}");
            }

            return new ValueTask();

        }

        protected override ValueTask OnDisconnectAsync()
        {
            return new ValueTask();
        }

        protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
        {
            var table = "--unknown--";
            try
            {
                Stopwatch sw;
                switch (data)
                {
                    case InitTableContract initTableContract:
                        table = initTableContract.TableName;
                        Console.WriteLine($"[NoSql][{_appName}] receive Init packet. table: {initTableContract.TableName}  size: {initTableContract.Data.Length}");
                        sw = Stopwatch.StartNew();
                        _subscriber.HandleInitTableEvent(initTableContract.TableName, initTableContract.Data);
                        sw.Stop();
                        if (sw.ElapsedMilliseconds > 2000)
                        {
                            Console.WriteLine($"[NoSql][{_appName}][Warning] LONG Init PACKET HANDLE {sw.ElapsedMilliseconds} ms; table: {initTableContract.TableName}");
                        }
                        break;

                    case InitPartitionContract initPartitionContract:
                        table = initPartitionContract.TableName;
                        Console.WriteLine($"[NoSql][{_appName}] receive InitPartition packet. table: {initPartitionContract.TableName}  size: {initPartitionContract.Data.Length}");
                        sw = Stopwatch.StartNew();
                        _subscriber.HandleInitPartitionEvent(initPartitionContract.TableName,
                            initPartitionContract.PartitionKey,
                            initPartitionContract.Data);
                        sw.Stop();
                        if (sw.ElapsedMilliseconds > 2000)
                        {
                            Console.WriteLine($"[NoSql][{_appName}][Warning] LONG InitPartition PACKET HANDLE  {sw.ElapsedMilliseconds} ms; table: {initPartitionContract.TableName}");
                        }
                        break;

                    case UpdateRowsContract updateRowsContract:
                        table = updateRowsContract.TableName;
                        sw = Stopwatch.StartNew();
                        _subscriber.HandleUpdateRowEvent(updateRowsContract.TableName, updateRowsContract.Data);
                        sw.Stop();
                        if (sw.ElapsedMilliseconds > 2000)
                        {
                            Console.WriteLine($"[NoSql][{_appName}][Warning] LONG UpdateRows PACKET HANDLE {sw.ElapsedMilliseconds} ms; table: {updateRowsContract.TableName}");
                        }
                        break;

                    case DeleteRowsContract deleteRowsContract:
                        table = deleteRowsContract.TableName;
                        sw = Stopwatch.StartNew();
                        _subscriber.HandleDeleteRowEvent(deleteRowsContract.TableName, deleteRowsContract.RowsToDelete);
                        sw.Stop();
                        if (sw.ElapsedMilliseconds > 2000)
                        {
                            Console.WriteLine($"[NoSql][{_appName}][Warning] LONG DeleteRows PACKET HANDLE {sw.ElapsedMilliseconds} ms; table: {deleteRowsContract.TableName}");
                        }
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[NoSql][{_appName}] There is a problem with Packet: {data.GetType().Name}; Table: {table}\n{e}");
                throw new Exception($"[NoSql][{_appName}] There is a problem with Packet: {data.GetType().Name}; Table: {table}", e);
            }

            return new ValueTask();
        }

        protected override IMyNoSqlTcpContract GetPingPacket()
        {
            return PingContract.Instance;
        }
    }
}