﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore.Storage
{
    public class CapEFDbTransaction : IDbContextTransaction
    {
        private readonly ICapTransaction _transaction;

        public CapEFDbTransaction(ICapTransaction transaction)
        {
            _transaction = transaction;
            var dbContextTransaction = (IDbContextTransaction)_transaction.DbTransaction;
            TransactionId = dbContextTransaction.TransactionId;
        }

        public void Dispose()
        {
            _transaction.Dispose();
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            return _transaction.CommitAsync(cancellationToken);
        }

        public Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            return _transaction.CommitAsync(cancellationToken);
        }

        public Guid TransactionId { get; }

        public ValueTask DisposeAsync()
        {
            return new ValueTask(Task.Run(() => _transaction.Dispose()));
        }


        public DbTransaction Instance
        {
            get
            {
                var dbContextTransaction = (IDbContextTransaction)_transaction.DbTransaction!;
                return dbContextTransaction.GetDbTransaction();
            }
        }
    }
}
