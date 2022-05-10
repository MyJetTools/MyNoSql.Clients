using System;
using Microsoft.Extensions.Logging;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlServer.DataReader
{
    public class MyNoSqlTcpClient : MyNoSqlSubscriber
    {
        private ILogger _logger;
        private readonly MyClientTcpSocket<IMyNoSqlTcpContract> _tcpClient;
        
        public MyNoSqlTcpClient(Func<string> getHostPort, string appName)
        {
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
                _logger.LogInformation($"[{name}][MuNoSql] {msg}");
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
    }
}