using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using MyNoSqlServer.Abstractions;
using Newtonsoft.Json;

namespace MyNoSqlServer.DataReader
{

    public class MyNoSqlReadRepository<T> : IMyNoSqlServerDataReader<T> where T : IMyNoSqlDbEntity
    {
        private readonly string _tableName;
        private readonly ILogger<MyNoSqlReadRepository<T>> _logger;

        private SortedDictionary<string, DataReaderPartition<T>> _cache =
            new SortedDictionary<string, DataReaderPartition<T>>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public MyNoSqlReadRepository(IMyNoSqlSubscriber subscriber, string tableName)
        {
            _tableName = tableName;
            tableName = tableName.ToLower();
            subscriber.Subscribe<T>(tableName, Init, InitPartition, Update, Delete);
        }
        
        public MyNoSqlReadRepository(IMyNoSqlSubscriber subscriber, string tableName, ILogger<MyNoSqlReadRepository<T>> logger)
            : this(subscriber, tableName)
        {
            _logger = logger;
        }


        private void Init(IReadOnlyList<T> items)
        {
            _logger?.LogInformation($"[NoSql] Start Init action for table: {_tableName}, count items: {items.Count}");
            try
            {



                IReadOnlyList<T> updated;
                IReadOnlyList<T> deleted;

                _lock.EnterWriteLock();
                try
                {
                    var oldOne = _cache;
                    _cache = new SortedDictionary<string, DataReaderPartition<T>>();

                    foreach (var item in items)
                    {
                        if (string.IsNullOrEmpty(item.PartitionKey))
                        {
                            _logger?.LogError($"[NoSql][Init] Cannot store entity with null or empty partition key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (string.IsNullOrEmpty(item.RowKey))
                        {
                            _logger?.LogError($"[NoSql][Init] Cannot store entity with null or empty row key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (!_cache.ContainsKey(item.PartitionKey))
                            _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                        var partition = _cache[item.PartitionKey];

                        partition.Update(item);
                    }

                    (updated, deleted) = oldOne.GetTotalDifference(_cache);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }


                if (updated != null)
                    NotifyChanged(updated);

                if (deleted != null)
                    NotifyDeleted(deleted);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"[NoSql] Cannot execute Init action for table: {_tableName}, count items: {items.Count}");
                throw;
            }

            _logger?.LogInformation($"[NoSql] Finish Init action for table: {_tableName}, count items: {items.Count}");
        }

        private void InitPartition(string partitionKey, IReadOnlyList<T> items)
        {
            _logger?.LogInformation($"[NoSql] Start Init Partition '{partitionKey}' action for table: {_tableName}, count items: {items.Count}");
            try
            {

                IReadOnlyList<T> updated;
                IReadOnlyList<T> deleted;

                _lock.EnterWriteLock();
                try
                {

                    var oldPartition = _cache.ContainsKey(partitionKey)
                        ? _cache[partitionKey]
                        : null;

                    _cache[partitionKey] = new DataReaderPartition<T>();

                    foreach (var item in items)
                    {
                        if (string.IsNullOrEmpty(item.PartitionKey))
                        {
                            _logger?.LogError($"[NoSql][InitPartition] Cannot store entity with null or empty partition key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (string.IsNullOrEmpty(item.RowKey))
                        {
                            _logger?.LogError($"[NoSql][InitPartition] Cannot store entity with null or empty row key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (!_cache.ContainsKey(item.PartitionKey))
                            _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                        var partition = _cache[item.PartitionKey];

                        partition.Update(item);
                    }

                    if (oldPartition == null)
                    {
                        NotifyChanged(_cache[partitionKey].GetRows().ToList());
                        return;
                    }

                    (updated, deleted) = oldPartition.FindDifference(_cache[partitionKey]);



                }
                finally
                {
                    _lock.ExitWriteLock();
                }


                if (updated != null)
                    NotifyChanged(updated);

                if (deleted != null)
                    NotifyDeleted(deleted);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"[NoSql] Cannot execute Init Partition '{partitionKey}' action for table: {_tableName}, count items: {items.Count}");
                throw;
            }

            _logger?.LogInformation($"[NoSql] Finish Init Partition '{partitionKey}' action for table: {_tableName}, count items: {items.Count}");
        }

        private void Update(IReadOnlyList<T> items)
        {
            try
            {
                _lock.EnterWriteLock();
                try
                {
                    foreach (var item in items)
                    {
                        if (string.IsNullOrEmpty(item.PartitionKey))
                        {
                            _logger?.LogError($"[NoSql][Update] Cannot store entity with null or empty partition key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (string.IsNullOrEmpty(item.RowKey))
                        {
                            _logger?.LogError($"[NoSql][Update] Cannot store entity with null or empty row key. Table: {_tableName}: {JsonConvert.SerializeObject(item)}");
                            continue;
                        }
                        
                        if (!_cache.ContainsKey(item.PartitionKey))
                            _cache.Add(item.PartitionKey, new DataReaderPartition<T>());

                        var partition = _cache[item.PartitionKey];

                        partition.Update(item);
                    }



                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                NotifyChanged(items);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"[NoSql] Cannot execute Update action for table: {_tableName}, count items: {items.Count}");
                throw;
            }
        }


        private void Delete(IEnumerable<(string partitionKey, string rowKey)> dataToDelete)
        {
            try
            {
                List<T> deleted = null;

                _lock.EnterWriteLock();
                try
                {

                    foreach (var (partitionKey, rowKey) in dataToDelete)
                    {
                        if (!_cache.ContainsKey(partitionKey))
                            continue;

                        var partition = _cache[partitionKey];


                        if (partition.TryDelete(rowKey, out var deletedItem))
                        {
                            deleted ??= new List<T>();
                            deleted.Add(deletedItem);
                        }

                        if (partition.Count == 0)
                            _cache.Remove(partitionKey);
                    }


                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                NotifyDeleted(deleted);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"[NoSql] Cannot execute Delete action for table: {_tableName}");
                throw;
            }
        }


        public T Get(string partitionKey, string rowKey)
        {
            _lock.EnterReadLock();
            try
            {

                if (!_cache.ContainsKey(partitionKey))
                    return default;

                var partition = _cache[partitionKey];

                return partition.TryGetRow(rowKey);

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }


        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, int skip, int take, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Where(condition).Skip(skip).Take(take).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(string partitionKey, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                if (!_cache.ContainsKey(partitionKey))
                    return Array.Empty<T>();

                return _cache[partitionKey].GetRows().Where(condition).ToList();

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlyList<T> Get(Func<T, bool> condition = null)
        {

            var result = new List<T>();
            _lock.EnterReadLock();

            try
            {
                if (condition == null)
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.GetRows());
                }
                else
                {
                    foreach (var rows in _cache.Values)
                        result.AddRange(rows.GetRows().Where(condition));
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return result;
        }

        private int _count;
        
        public int Count()
        {
            return _count;
        }
        

        public int Count(string partitionKey)
        {
            _lock.EnterReadLock();

            try
            {
                return _cache.ContainsKey(partitionKey) ? _cache[partitionKey].Count : 0;
            }
            finally
            {
                _lock.ExitReadLock();
            }

        }
        
        public int Count(string partitionKey, Func<T, bool> condition)
        {
            _lock.EnterReadLock();
            try
            {
                return _cache.ContainsKey(partitionKey) ? _cache[partitionKey].GetRows().Count(condition) : 0;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }


        private readonly List<Action<IReadOnlyList<T>>> _changedActions = new List<Action<IReadOnlyList<T>>>();

        public IMyNoSqlServerDataReader<T> SubscribeToUpdateEvents(Action<IReadOnlyList<T>> updateSubscriber, Action<IReadOnlyList<T>> deleteSubscriber)
        {
            _changedActions.Add(updateSubscriber);
            _deletedActions.Add(deleteSubscriber);
            return this;
        }


        private void UpdateCount()
        {
            _count = 0;
            
            foreach (var rows in _cache.Values)
                _count += rows.Count;
        }
        
        private void NotifyChanged(IReadOnlyList<T> items)
        {
            UpdateCount();
            
            if (items == null)
                return;

            if (items.Count == 0)
                return;

            

            foreach (var changedAction in _changedActions)
                changedAction(items);
            

        }


        private readonly List<Action<IReadOnlyList<T>>> _deletedActions = new List<Action<IReadOnlyList<T>>>();



        private void NotifyDeleted(IReadOnlyList<T> items)
        {
            UpdateCount();
            
            if (items == null)
                return;

            if (items.Count == 0)
                return;

            foreach (var changedAction in _deletedActions)
                changedAction(items);
        }
    }


    public static class MyNoSqlReadRepositoryExtensions
    {
        public static MyNoSqlReadRepository<T> SubscribeToTable<T>(
            this IMyNoSqlSubscriber subscriber, string tableName) where T : IMyNoSqlDbEntity
        {
            return new MyNoSqlReadRepository<T>(subscriber, tableName);
        }
    }

}
