﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Oracle;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP
{
    public class OracleCapOptionsExtension : ICapOptionsExtension
    {
        private readonly Action<OracleOptions> _configure;

        public OracleCapOptionsExtension(Action<OracleOptions> configure)
        {
            _configure = configure;
        }

        public void AddServices(IServiceCollection services)
        {
            services.AddSingleton(new CapStorageMarkerService("Oracle"));
            services.Configure(_configure);
            services.AddSingleton<IConfigureOptions<OracleOptions>, ConfigureOracleOptions>();

            services.AddSingleton<IDataStorage, OracleDataStorage>();
            services.AddSingleton<IStorageInitializer, OracleStorageInitializer>();
            services.AddTransient<ICapTransaction, OracleCapTransaction>();

        }
    }
}
