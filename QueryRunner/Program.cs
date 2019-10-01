using Dapper;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
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

            var iterations = 4;
            var times = new double[iterations];

            for (var i = 0; i < iterations; i++)
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                var connectionString = sqlConnectionStringBuilder.ToString();
                var tasks = WriteMassiveBatch(connectionString, 1_000_000, Environment.ProcessorCount);
                await Task.WhenAll(tasks).ConfigureAwait(false);

                stopwatch.Stop();
                var elapsed = stopwatch.Elapsed.TotalSeconds;
                times[i] = elapsed;
                Console.WriteLine($"Elapsed Second: {elapsed} (Iteration {i})");
            }

            var average = times.Sum(q => q) / iterations;
            Console.WriteLine($"Average across {iterations} iterations: {average}");
        }

        private static DataTable CreateDataTable()
        {
            var table = new DataTable { TableName = "TargetTable" };
            var column1 = table.Columns.Add("Column1", typeof(string));
            column1.MaxLength = 50;

            var column2 = table.Columns.Add("Column2", typeof(string));
            column2.MaxLength = 50;

            var column3 = table.Columns.Add("Column3", typeof(string));
            column3.MaxLength = 50;
            return table;
        }

        private static List<Task> WriteMassiveBatch(string connectionString, int rowsToInsert, int threads = 1)
        {
            var tasks = new List<Task>();
            for (int thread = 0; thread < threads; thread++)
            {
                var task = Task.Run(() =>
                {
                    using var dataTable = CreateDataTable();
                    for (var i = thread; i < rowsToInsert; i += threads)
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
                        WriteDataUsingBatchRpc(dataTable, sqlConnection);
                        //WriteDataUsingTvp(dataTable, sqlConnection);

                        dataTable.Clear();
                    }
                });

                tasks.Add(task);
            }

            return tasks;
        }

        private static void WriteDataUsingBatchRpc(DataTable dataTable, SqlConnection sqlConnection)
        {
            using (var sqlTransaction = sqlConnection.BeginTransaction())
            {
                using (var sqlDataAdapter = new SqlDataAdapter())
                {
                    using var sqlCommand = GetSqlCommandForBatchRpc(dataTable.TableName);
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.Transaction = sqlTransaction;
                    sqlDataAdapter.InsertCommand = sqlCommand;
                    sqlDataAdapter.UpdateBatchSize = 1000;
                    sqlDataAdapter.Update(dataTable);
                    sqlTransaction.Commit();
                }
            }
        }

        private static void WriteDataUsingTvp(DataTable dataTable, SqlConnection sqlConnection)
        {
            using var sqlCommand = GetSqlCommandForTvp(dataTable.TableName, dataTable);
            sqlCommand.Connection = sqlConnection;
            sqlCommand.ExecuteNonQuery();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        private static SqlCommand GetSqlCommandForBatchRpc(string tableName)
        {
            var sqlCommand = new SqlCommand
            {
                CommandTimeout = 0,
                CommandText = "Append" + tableName,
                CommandType = CommandType.StoredProcedure,
                UpdatedRowSource = UpdateRowSource.None
            };
            sqlCommand.Parameters.Add("@Column1", SqlDbType.NVarChar, -1, "Column1");
            sqlCommand.Parameters.Add("@Column2", SqlDbType.NVarChar, -1, "Column2");
            sqlCommand.Parameters.Add("@Column3", SqlDbType.NVarChar, -1, "Column3");

            return sqlCommand;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        private static SqlCommand GetSqlCommandForTvp(string tableName, DataTable dataTable)
        {
            var sqlCommand = new SqlCommand
            {
                CommandTimeout = 0,
                CommandText = $"Append{tableName}WithTvp",
                CommandType = CommandType.StoredProcedure,
                UpdatedRowSource = UpdateRowSource.None
            };
            var dataParameter = sqlCommand.Parameters.Add("@Data", SqlDbType.Structured);
            dataParameter.Value = dataTable;
            return sqlCommand;
        }
    }
}
