using System;
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

                Console.WriteLine("Subscribed to MyNoSql table: " + tableToSubscribe);
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
                switch (data)
                {
                    case InitTableContract initTableContract:
                        table = initTableContract.TableName;
                        Console.WriteLine($"[NoSql] receive Init packet. table: {initTableContract.TableName}  size: {initTableContract.Data.Length}");
                        _subscriber.HandleInitTableEvent(initTableContract.TableName, initTableContract.Data);
                        break;

                    case InitPartitionContract initPartitionContract:
                        table = initPartitionContract.TableName;
                        Console.WriteLine($"[NoSql] receive InitPartition packet. table: {initPartitionContract.TableName}  size: {initPartitionContract.Data.Length}");
                        _subscriber.HandleInitPartitionEvent(initPartitionContract.TableName,
                            initPartitionContract.PartitionKey,
                            initPartitionContract.Data);
                        break;

                    case UpdateRowsContract updateRowsContract:
                        table = updateRowsContract.TableName;
                        _subscriber.HandleUpdateRowEvent(updateRowsContract.TableName, updateRowsContract.Data);
                        break;

                    case DeleteRowsContract deleteRowsContract:
                        table = deleteRowsContract.TableName;
                        _subscriber.HandleDeleteRowEvent(deleteRowsContract.TableName, deleteRowsContract.RowsToDelete);
                        break;

                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"There is a problem with Packet: {data.GetType().Name}; Table: {table}\n{e}");
                throw new Exception($"There is a problem with Packet: {data.GetType().Name}; Table: {table}", e);
            }

            return new ValueTask();
        }

        protected override IMyNoSqlTcpContract GetPingPacket()
        {
            return PingContract.Instance;
        }
    }
}