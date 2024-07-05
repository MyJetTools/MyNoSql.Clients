using System;
using Microsoft.Extensions.Logging;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlTcpClient : MyNoSqlSubscriber
    {
        private readonly Func<string> _getHostPort;
        private readonly string _appName;
        private ILogger _logger;
        private MyClientTcpSocket<IMyNoSqlTcpContract> _tcpClient;
        
        public MyNoSqlTcpClient(Func<string> getHostPort, string appName)
        {
            _getHostPort = getHostPort;
            _appName = appName;
            _tcpClient = new MyClientTcpSocket<IMyNoSqlTcpContract>(getHostPort, TimeSpan.FromSeconds(3));

            _tcpClient
                .RegisterTcpContextFactory(() => new MyNoSqlServerClientTcpContext(this, appName))
                .Logs.AddLogInfo((c, m) => Console.WriteLine("MyNoSql: " + m))
                .Logs.AddLogException((c, m) => Console.WriteLine("MyNoSql: " + m))
                .RegisterTcpSerializerFactory(() => new MyNoSqlTcpSerializer());
        }

        public MyNoSqlTcpClient AddLogInfo(Action<ITcpContext, string> logInfo)
        {
            _tcpClient.Logs.AddLogInfo(logInfo);
            return this;
        }
        
        public MyNoSqlTcpClient AddLogException(Action<ITcpContext, Exception> logException)
        {
            _tcpClient.Logs.AddLogException(logException);
            return this;
        }
        
        public void AddLogger(ILogger logger, string name = "")
        {
            _logger = logger;
            
            _tcpClient.Logs.AddLogException((c, e) =>
            {
                _logger.LogError(e, $"[{name}] Error from MyNoSqlTcp. ConnectId: {c?.Id}");
            });


            _tcpClient.Logs.AddLogInfo((c, msg) =>
            {
                _logger.LogInformation($"[{name}][MyNoSql] {msg}");
            });
        }

        public bool Connected => _tcpClient.Connected;

        public long ConnectionId
        {
            get
            {
                var currentContext = _tcpClient.CurrentTcpContext;

                if (currentContext == null)
                    return -1;

                return currentContext.Id;
            }
        }

        public void Start()
        {
            _tcpClient.Start();
        }
        
        public void Stop()
        {
            _tcpClient.Stop();
        }

        public void ReCreateAndStart()
        {
            _tcpClient?.Stop();
            
            _tcpClient =  new MyClientTcpSocket<IMyNoSqlTcpContract>(_getHostPort, TimeSpan.FromSeconds(3));

            _tcpClient
                .RegisterTcpContextFactory(() => new MyNoSqlServerClientTcpContext(this, _appName))
                .Logs.AddLogInfo((c, m) => Console.WriteLine("MyNoSql: " + m))
                .Logs.AddLogException((c, m) => Console.WriteLine("MyNoSql: " + m))
                .RegisterTcpSerializerFactory(() => new MyNoSqlTcpSerializer());
            
            _tcpClient.Start();
            
            _logger?.LogError($"=== NOSQL ARE RESTARTED!!! ===");
        }
    }
}