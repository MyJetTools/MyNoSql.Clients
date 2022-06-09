using System;

namespace MyNoSqlServer.DataWriter.Exceptions
{
    public class MyNoSqlArgumentsException: Exception
    {
        public MyNoSqlArgumentsException(string message) : base(message)
        {
        
        }
    }
}