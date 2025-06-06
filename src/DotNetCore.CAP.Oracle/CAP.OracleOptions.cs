﻿// Copyright (c) .NET Core Community. All rights reserved.
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
    }
}
