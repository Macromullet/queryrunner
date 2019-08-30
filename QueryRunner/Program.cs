using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace QueryRunner
{
    class Program
    {
        private static readonly string _query =
@"SELECT TOP 1 
     [member].[member_no],
     [member].[lastname],
     [payment].[payment_no],
     [payment].[payment_dt],
     [payment].[payment_amt]
FROM [dbo].[payment]
INNER JOIN [dbo].[member]
ON [member].[member_no] = [payment].[member_no];";

        async static Task Main(string[] args)
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "(local)",
                InitialCatalog = "Test",
                IntegratedSecurity = true
            };

            var iterations = 5;
            var times = new double[iterations];

            for (var i = 0; i < iterations; i++)
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                var connectionString = sqlConnectionStringBuilder.ToString();
                //await RunQueryInParallelAsync(connectionString, _query, 100000, 8);
                await WriteMassiveBatch(connectionString, 1_000_000);

                stopwatch.Stop();
                var elapsed = stopwatch.Elapsed.TotalSeconds;
                times[i] = elapsed;
                Console.Write($"Elapsed Second: {elapsed} (Iteration {i})");
            }

            var average = times.Sum(q => q) / iterations;
            Console.WriteLine($"Average across {iterations} iterations: {average}");
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
            var dataTable = CreateDataTable();
            for (int i=0;i<rowsToInsert;i++)
            {
                DataRow dataRow = dataTable.NewRow();
                var indexString = i.ToString();
                dataRow[0] = "rowa" + indexString;
                dataRow[1] = "rowb" + indexString;
                dataRow[2] = "rowc" + indexString;
                dataTable.Rows.Add(dataRow);
            }
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                sqlConnection.Execute("TRUNCATE TABLE TargetTable");
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    using (var sqlDataAdapter = new SqlDataAdapter())
                    {
                        sqlDataAdapter.UpdateBatchSize = 1000;
                        using (var sqlCommand = GetSqlCommand(dataTable.TableName))
                        {
                            sqlCommand.Connection = sqlConnection;
                            sqlCommand.Transaction = sqlTransaction;
                            sqlDataAdapter.InsertCommand = sqlCommand;
                            sqlDataAdapter.Update(dataTable);
                            sqlTransaction.Commit();
                        }
                    }

                    dataTable.Clear();
                }
            }

            return Task.CompletedTask;
        }

        private static SqlCommand GetSqlCommand(string tableName)
        {
            var sqlCommand = new SqlCommand();
            sqlCommand.CommandText = "Append" + tableName;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.UpdatedRowSource = UpdateRowSource.None;
            sqlCommand.Parameters.Add("@Column1", SqlDbType.NVarChar, 50, "Column1");
            sqlCommand.Parameters.Add("@Column2", SqlDbType.NVarChar, 50, "Column2");
            sqlCommand.Parameters.Add("@Column3", SqlDbType.NVarChar, 50, "Column3");

            return sqlCommand;
        }

        private static async Task RunQueryInParallelAsync(string connectionString, string query, int count, int cores)
        {
            var activeTasks = new List<Task>();
            for (int coreNumber = 0; coreNumber < cores; coreNumber++)
            {
                var task = RunQueryAsync(connectionString, query, count);
                activeTasks.Add(task);
            }

            await Task.WhenAll(activeTasks.ToArray());
        }

        private static async Task RunQueryAsync(string connectionString, string query, int count)
        {
            using (var dbConnection = new SqlConnection(connectionString))
            {
                for (int i = 0; i < count; i++)
                {
                    await dbConnection.ExecuteScalarAsync(query);
                }
            }
        }
    }
}
