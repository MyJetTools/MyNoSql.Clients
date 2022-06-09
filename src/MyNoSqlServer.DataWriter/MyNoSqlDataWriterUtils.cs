using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataWriter.Exceptions;
using Newtonsoft.Json;

namespace MyNoSqlServer.DataWriter
{
    public static class MyNoSqlDataWriterUtils
    {
        public static Url AppendDataSyncPeriod(this Url url, DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            return url.SetQueryParam("syncPeriod", dataSynchronizationPeriod.AsString(null));
        }

        public static Url WithPartitionKeyAsQueryParam(this Url url, string partitionKey)
        {
            return url.SetQueryParam("partitionKey", partitionKey);
        }

        public static Url WithRowKeyAsQueryParam(this Url url, string rowKey)
        {
            return url.SetQueryParam("rowKey", rowKey);
        }


        public static IFlurlRequest AllowNonOkCodes(this Url url)
        {
            return url.AllowHttpStatus(HttpStatusCode.NotFound)
                .AllowHttpStatus(HttpStatusCode.Conflict);
        }

        public static Url WithTableNameAsQueryParam(this Url url, string tableName)
        {
            return url.SetQueryParam("tableName", tableName);
        }
        
        public static Url WithTransactionIdAsQueryParam(this Url url, string transactionId)
        {
            return url.SetQueryParam("transactionId", transactionId);
        }
        

        public static Url WithPersistTableAsQueryParam(this Url url, bool persist)
        {
            var value = persist ? "1" : "0";
            return url.SetQueryParam("persist", value);
        }


        public static async ValueTask<T> ReadAsJsonAsync<T>(this Task<HttpResponseMessage> responseTask)
        {
            var response = await responseTask;
            return await response.ReadAsJsonAsync<T>();
        }

        public static async ValueTask<T> ReadAsJsonAsync<T>(this HttpResponseMessage response)
        {
            var json = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        internal static async ValueTask<OperationResult> GetOperationResultCodeAsync(
            this IFlurlResponse httpResponseMessage)
        {
            switch (httpResponseMessage.StatusCode)
            {
                case 200:
                    return OperationResult.Ok;

                case 404:
                    return OperationResult.RecordNotFound;

                case 409:
                    var message = await httpResponseMessage.GetStringAsync();
                    return (OperationResult)int.Parse(message);

                default:
                    var messageUnknown = await httpResponseMessage.GetStringAsync();
                    throw new Exception(
                        $"Unknown HTTP result Code{httpResponseMessage.StatusCode}. Message: {messageUnknown}");
            }
        }

        public static void Validate<T>(this T entity) where T : IMyNoSqlDbEntity
        {
            if (entity == null)
            {
                var message =
                    $"Entity of type {typeof(T).Name} can not be null";
                Console.WriteLine(message);
                throw new MyNoSqlArgumentsException(message);
            }
            
            if (string.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                var message =
                    $"Entity of type {typeof(T).Name} has empty partition key, Entity: {JsonConvert.SerializeObject(entity)}";
                Console.WriteLine(message);
                throw new MyNoSqlArgumentsException(message);
            }
            
            if (string.IsNullOrWhiteSpace(entity.RowKey))
            {
                var message =
                    $"Entity of type {typeof(T).Name} has empty row key, Entity: {JsonConvert.SerializeObject(entity)}";
                Console.WriteLine(message);
                throw new MyNoSqlArgumentsException(message);
            }
        }

    }
}