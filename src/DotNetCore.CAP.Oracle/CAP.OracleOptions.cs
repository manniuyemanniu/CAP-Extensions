// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using System;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP
{
    public class OracleOptions : EFOptions
    {
        /// <summary>
        /// Gets or sets the database's connection string that will be used to store database entities.
        /// </summary>
        public string ConnectionString { get; set; }
        //public OracleDataSource? DataSource { get; set; }

        /// <summary>
        /// Creates an Npgsql connection from the configured data source.
        /// </summary>
        //        internal OracleConnection CreateConnection()
        //        {
        //#pragma warning disable CS0618 // Type or member is obsolete
        //            //return DataSource != null ? DataSource.CreateConnection() : new OracleConnection(ConnectionString);
        //            return new OracleConnection(ConnectionString);
        //#pragma warning restore CS0618 // Type or member is obsolete
        //        }
    }

    internal class ConfigureOracleOptions : IConfigureOptions<OracleOptions>
    {
        private readonly IServiceScopeFactory _serviceScopeFactory;

        public ConfigureOracleOptions(IServiceScopeFactory serviceScopeFactory)
        {
            _serviceScopeFactory = serviceScopeFactory;
        }

        public void Configure(OracleOptions options)
        {    
            //Kylin 20240815 新增 
            if (!string.IsNullOrWhiteSpace(options.ConnectionString))
            {
                string Schema = ExtractUserId(options.ConnectionString);
                if (!string.IsNullOrWhiteSpace(Schema))
                {
                    options.Schema = Schema;
                }
            }
            if (options.DbContextType == null) return;

            if (Helper.IsUsingType<ICapPublisher>(options.DbContextType))
                throw new InvalidOperationException(
                    "We detected that you are using ICapPublisher in DbContext, please change the configuration to use the storage extension directly to avoid circular references! eg:  x.UseOracle()");
            using var scope = _serviceScopeFactory.CreateScope();
            var provider = scope.ServiceProvider;
            using var dbContext = (DbContext)provider.GetRequiredService(options.DbContextType);
            var connectionString = dbContext.Database.GetConnectionString();
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(connectionString);
            options.ConnectionString = connectionString;
         
        }
        /// <summary>
        /// 特定分割把Schema 进行替换位自身的User Id中的值  为空的时候就是默认的cap  否则就是User Id中用户
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public static string ExtractUserId(string connectionString)
        {
            // 分割连接字符串
            string[] parts = connectionString.Split(';');
            // 查找包含 "User Id" 的部分
            foreach (var part in parts)
            {
                if (part.StartsWith("User Id="))
                {
                    return part.Substring("User Id=".Length);
                }
            }
            return null;
        }
    }
}
