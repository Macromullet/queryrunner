using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading.Tasks;

namespace QueryRunner
{
    class Program
    {
        async static Task Main()
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "(local)",
                InitialCatalog = "Test",
                IntegratedSecurity = true
            };

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var connectionString = sqlConnectionStringBuilder.ToString();
            await WriteMassiveBatch(connectionString, 1_000_000).ConfigureAwait(false);

            stopwatch.Stop();
            var elapsed = stopwatch.Elapsed.TotalSeconds;
            Console.Write($"Elapsed Second: {elapsed}");

        }

        private static DataTable CreateDataTable()
        {
            var table = new DataTable { TableName = "TargetTable" };
            table.Columns.Add("Column1", typeof(string));
            table.Columns.Add("Column2", typeof(string));
            table.Columns.Add("Column3", typeof(string));
            return table;
        }

        private static Task WriteMassiveBatch(string connectionString, int rowsToInsert)
        {
            using var dataTable = CreateDataTable();
            for (int i=0;i<rowsToInsert;i++)
            {
                var dataRow = dataTable.NewRow();
                var indexString = i.ToString(CultureInfo.InvariantCulture);
                dataRow[0] = "rowa" + indexString;
                dataRow[1] = "rowb" + indexString;
                dataRow[2] = "rowc" + indexString;
                dataTable.Rows.Add(dataRow);
            }
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                sqlConnection.Execute("TRUNCATE TABLE TargetTable");
                using var sqlTransaction = sqlConnection.BeginTransaction();
                using (var sqlDataAdapter = new SqlDataAdapter())
                {
                    sqlDataAdapter.UpdateBatchSize = 1000;
                    using var sqlCommand = GetSqlCommand(dataTable.TableName);
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.Transaction = sqlTransaction;
                    sqlDataAdapter.InsertCommand = sqlCommand;
                    sqlDataAdapter.Update(dataTable);
                    sqlTransaction.Commit();
                }

                dataTable.Clear();
            }

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        private static SqlCommand GetSqlCommand(string tableName)
        {
            var sqlCommand = new SqlCommand
            {
                CommandText = "Append" + tableName,
                CommandType = CommandType.StoredProcedure,
                UpdatedRowSource = UpdateRowSource.None
            };
            sqlCommand.Parameters.Add("@Column1", SqlDbType.NVarChar, 50, "Column1");
            sqlCommand.Parameters.Add("@Column2", SqlDbType.NVarChar, 50, "Column2");
            sqlCommand.Parameters.Add("@Column3", SqlDbType.NVarChar, 50, "Column3");

            return sqlCommand;
        }
    }
}
