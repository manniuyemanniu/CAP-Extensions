// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Oracle
{
    public class OracleDataStorage : IDataStorage
    {
        private readonly IOptions<CapOptions> _capOptions;
        private readonly IOptions<OracleOptions> _options;
        private readonly IStorageInitializer _initializer;
        private readonly ISerializer _serializer;

        private readonly string _pubName;
        private readonly string _recName;

        public OracleDataStorage(
            IOptions<OracleOptions> options,
            IOptions<CapOptions> capOptions,
            IStorageInitializer initializer,
            ISerializer serializer)
        {
            _capOptions = capOptions;
            _options = options;
            _initializer = initializer;
            _serializer = serializer;
            _pubName = initializer.GetPublishedTableName();
            _recName = initializer.GetReceivedTableName();
        }


        public async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
        {
            await ChangeMessageStateAsync(_recName, message, state).ConfigureAwait(false);
        }


        public async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000,
            CancellationToken token = default)
        {
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);



            //var sql = $"DELETE FROM {table} WHERE \"ExpiresAt\" < :timeout AND \"Id\" IN (SELECT \"Id\" FROM {table} WHERE ROWNUM<= :batchCount)";

            var sql = $"DELETE FROM {table} WHERE \"Id\" IN (SELECT \"Id\" FROM {table} WHERE \"ExpiresAt\" < :timeout AND (\"StatusName\"='{StatusName.Succeeded}' OR \"StatusName\"='{StatusName.Failed}')) AND ROWNUM<= :batchCount";

            var count = await connection.ExecuteNonQueryAsync(
               sql, null,
                new OracleParameter(":timeout", timeout),
                new OracleParameter(":batchCount", batchCount));
            return await Task.FromResult(count);
        }

        public async Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry()
        {
            return await GetMessagesOfNeedRetryAsync(_pubName).ConfigureAwait(false);
        }

        public async Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry()
        {
            return await GetMessagesOfNeedRetryAsync(_recName).ConfigureAwait(false);
        }

        public IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(_options, _initializer, _serializer);
        }

        private async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state)
        {
            var sql = $"UPDATE {tableName} SET \"Content\"=:Content,\"Retries\"=:Retries,\"ExpiresAt\"=:ExpiresAt,\"StatusName\"=:StatusName WHERE \"Id\"=:Id";

            object[] sqlParams =
            {
                new OracleParameter(":Id", long.Parse(message.DbId)),
                new OracleParameter(":Content", _serializer.Serialize(message.Origin)),
                new OracleParameter(":Retries", message.Retries),
                //new OracleParameter(":ExpiresAt",message.ExpiresAt.HasValue?(object)message.ExpiresAt:DBNull.Value),
                new OracleParameter(":ExpiresAt", message.ExpiresAt),
                new OracleParameter(":StatusName", state.ToString("G")),
            };

            using var connection = new OracleConnection(_options.Value.ConnectionString);
            connection.ExecuteNonQuery(sql, sqlParams: sqlParams);

            await Task.CompletedTask;
        }

        private async Task StoreReceivedMessage(object[] sqlParams)
        {
            //var sql = $"INSERT INTO {_recName} (\"Id\",\"Version\",\"Name\",\"Group\",\"Content\",\"Retries\",\"Added\",\"ExpiresAt\",\"StatusName\")" +
            //    $" VALUES (:Id,'{_capOptions.Value.Version}',:Name,:Group1,:Content,:Retries,:Added,:ExpiresAt,:StatusName) ";
            //using var connection = new OracleConnection(_options.Value.ConnectionString);
            //connection.ExecuteNonQuery(sql, sqlParams: sqlParams);
            var sql =
            $"INSERT INTO {_recName}(\"Id\",\"Version\",\"Name\",\"Group\",\"Content\",\"Retries\",\"Added\",\"ExpiresAt\",\"StatusName\")" +
            $"VALUES(:Id,'{_capOptions.Value.Version}',:Name,:Group,:Content,:Retries,:Added,:ExpiresAt,:StatusName) RETURNING \"Id\";";

            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }

        private async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName)
        {
            var fourMinAgo = DateTime.Now.AddMinutes(-4).ToString("O");
            var sql = $"SELECT \"Id\",\"Content\",\"Retries\",\"Added\" FROM {tableName} WHERE \"Retries\"<:Retries " +
                $"AND \"Version\"=:Version AND \"Added\"<:Added AND (\"StatusName\"='{StatusName.Failed}' OR \"StatusName\"='{StatusName.Scheduled}') AND ROWNUM<= 200";

            object[] sqlParams =
            {
                new OracleParameter(":Retries", _capOptions.Value.FailedRetryCount),
                new OracleParameter(":Version", _capOptions.Value.Version),
                new OracleParameter(":Added", fourMinAgo)
            };

            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            var result = await connection.ExecuteReaderAsync(sql, async reader =>
            {
                var messages = new List<MediumMessage>();
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    messages.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = _serializer.Deserialize(reader.GetString(1))!,
                        Retries = reader.GetInt32(2),
                        Added = reader.GetDateTime(3)
                    });
                }

                return messages;
            }, sqlParams: sqlParams).ConfigureAwait(false);

            return await Task.FromResult(result);
        }

        private async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state,
            object? transaction = null)
        {
            var sql =
                $"UPDATE {tableName} SET \"Content\"=:Content,\"Retries\"=:Retries,\"ExpiresAt\"=:ExpiresAt,\"StatusName\"=:StatusName WHERE \"Id\"=:Id";

            object[] sqlParams =
            {
                new OracleParameter(":Id", long.Parse(message.DbId)),
                new OracleParameter(":Content", _serializer.Serialize(message.Origin)),
                new OracleParameter(":Retries", message.Retries),
                new OracleParameter(":ExpiresAt", message.ExpiresAt),
                new OracleParameter(":StatusName", state.ToString("G"))
        };

            if (transaction is DbTransaction dbTransaction)
            {
                var connection = (OracleConnection)dbTransaction.Connection!;
                await connection.ExecuteNonQueryAsync(sql, dbTransaction, sqlParams).ConfigureAwait(false);
            }
            else
            {
                await using var connection = new OracleConnection(_options.Value.ConnectionString);
                await using var _ = connection.ConfigureAwait(false);
                await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }
        }

        public async Task ChangePublishStateToDelayedAsync(string[] ids)
        {
            var sql =
            $"UPDATE {_pubName} SET \"StatusName\"='{StatusName.Delayed}' WHERE \"Id\" IN ({string.Join(',', ids)});";
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            await connection.ExecuteNonQueryAsync(sql).ConfigureAwait(false);
        }

        public async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object transaction = null)
        {
            await ChangeMessageStateAsync(_pubName, message, state, transaction).ConfigureAwait(false);
        }

        public async Task<MediumMessage> StoreMessageAsync(string name, Message content, object transaction = null)
        {
            var sql =
            $"INSERT INTO {_pubName} (\"Id\",\"Version\",\"Name\",\"Content\",\"Retries\",\"Added\",\"ExpiresAt\",\"StatusName\")" +
            $"VALUES(:Id,'{_options.Value.Version}',:Name,:Content,:Retries,:Added,:ExpiresAt,:StatusName);";

            var message = new MediumMessage
            {
                DbId = content.GetId(),
                Origin = content,
                Content = _serializer.Serialize(content),
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

            object[] sqlParams =
            {
                new OracleParameter(":Id", long.Parse(message.DbId)),
                new OracleParameter(":Name", name),
                new OracleParameter(":Content", message.Content),
                new OracleParameter(":Retries", message.Retries),
                new OracleParameter(":Added", message.Added),
                new OracleParameter(":ExpiresAt", message.ExpiresAt.HasValue ? message.ExpiresAt.Value : DBNull.Value),
                new OracleParameter(":StatusName", nameof(StatusName.Scheduled))
        };

            if (transaction == null)
            {
                var connection = new OracleConnection(_options.Value.ConnectionString);
                await using var _ = connection.ConfigureAwait(false);
                await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            }
            else
            {
                var dbTrans = transaction as DbTransaction;
                if (dbTrans == null && transaction is IDbContextTransaction dbContextTrans)
                    dbTrans = dbContextTrans.GetDbTransaction();

                var conn = dbTrans?.Connection!;
                await conn.ExecuteNonQueryAsync(sql, dbTrans, sqlParams).ConfigureAwait(false);
            }

            return message;
        }

        public async Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
        {
            object[] sqlParams =
            {
                    new OracleParameter(":Id", SnowflakeId.Default().NextId()),
                    new OracleParameter(":Name", name),
                    new OracleParameter(":Group", group),
                    new OracleParameter(":Content", content),
                    new OracleParameter(":Retries", _capOptions.Value.FailedRetryCount),
                    new OracleParameter(":Added", DateTime.Now),
                    new OracleParameter(":ExpiresAt", DateTime.Now.AddSeconds(_capOptions.Value.FailedMessageExpiredAfter)),
                    new OracleParameter(":StatusName", nameof(StatusName.Failed))
            };

            await StoreReceivedMessage(sqlParams).ConfigureAwait(false);
        }

        public async Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message content)
        {
            var mdMessage = new MediumMessage
            {
                DbId = SnowflakeId.Default().NextId().ToString(),
                Origin = content,
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };

            object[] sqlParams =
            {
                new OracleParameter(":Id", long.Parse(mdMessage.DbId)),
                new OracleParameter(":Name", name),
                new OracleParameter(":Group", group),
                new OracleParameter(":Content", _serializer.Serialize(mdMessage.Origin)),
                new OracleParameter(":Retries", mdMessage.Retries),
                new OracleParameter(":Added", mdMessage.Added),
                new OracleParameter(":ExpiresAt", mdMessage.ExpiresAt.HasValue ? mdMessage.ExpiresAt.Value : DBNull.Value),
                new OracleParameter(":StatusName", nameof(StatusName.Scheduled))
        };

            await StoreReceivedMessage(sqlParams).ConfigureAwait(false);

            return mdMessage;
        }

        public async Task ScheduleMessagesOfDelayedAsync(Func<object, IEnumerable<MediumMessage>, Task> scheduleTask, CancellationToken token = default)
        {
            var sql =
            $"SELECT \"Id\",\"Content\",\"Retries\",\"Added\",\"ExpiresAt\" FROM {_pubName} WHERE \"Version\"=:Version " +
            $"AND ((\"ExpiresAt\"< :TwoMinutesLater AND \"StatusName\" = '{StatusName.Delayed}') OR (\"ExpiresAt\"< :OneMinutesAgo AND \"StatusName\" = '{StatusName.Queued}')) FOR UPDATE";
            //--SKIP LOCKED;
            var sqlParams = new object[]
            {
                new OracleParameter(":Version", _capOptions.Value.Version),
                new OracleParameter(":TwoMinutesLater", DateTime.Now.AddMinutes(2)),
                new OracleParameter(":OneMinutesAgo", DateTime.Now.AddMinutes(-1))
            };

            await using var connection = new OracleConnection(_options.Value.ConnectionString);
            await connection.OpenAsync(token);
            await using var transaction = await connection.BeginTransactionAsync(token);
            var messageList = await connection.ExecuteReaderAsync(sql, async reader =>
            {
                var messages = new List<MediumMessage>();
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    messages.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = _serializer.Deserialize(reader.GetString(1))!,
                        Retries = reader.GetInt32(2),
                        Added = reader.GetDateTime(3),
                        ExpiresAt = reader.GetDateTime(4)
                    });
                }

                return messages;
            }, transaction, sqlParams).ConfigureAwait(false);

            await scheduleTask(transaction, messageList);

            await transaction.CommitAsync(token);
        }
    }
}
