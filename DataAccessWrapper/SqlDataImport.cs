using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;

namespace DataAccessWrapper
{
    /// <summary>
    /// Provides methods for importing data into a SqlServer database using SqlBulkCopy
    /// </summary>
    public class SqlDataImport : DataImport
    {
        protected string cs;

        public SqlDataImport(string connectionString)
        {
            cs = connectionString;
        }

        public void ImportDataReader(DbDataReader reader, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            ImportDataReaderInternal(reader, destTable, truncateDestTable, columnMapping, 50000);
        }

        public void ImportDataReader(DbDataReader reader, string destTable, bool truncateDestTable, int batchSize)
        {
            ImportDataReaderInternal(reader, destTable, truncateDestTable, null, batchSize);
        }

        public void ImportDataReader(DbDataReader reader, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping, int batchSize)
        {
            ImportDataReaderInternal(reader, destTable, truncateDestTable, columnMapping, batchSize);
        }

        public void ImportDataReader(DbDataReader reader, string destTable, bool truncateDestTable)
        {
            ImportDataReaderInternal(reader, destTable, truncateDestTable, null, 50000);
        }

        protected virtual void ImportDataReaderInternal(DbDataReader reader, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping, int batchSize)
        {
            OnImportStarting(new DataImportStartingArgs() { TableName = destTable });

            using (var bulk = new SqlBulkCopy(cs))
            {
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = destTable;
                bulk.NotifyAfter = batchSize;
                bulk.SqlRowsCopied += (s, e) => OnImportProgress(new DataImportProgressArgs() { TableName = destTable, RowsImported = batchSize, TotalRowsImported = (int)e.RowsCopied });

                if (columnMapping != null)
                {
                    foreach (var col in columnMapping)
                    {
                        bulk.ColumnMappings.Add(col.SourceColumn, col.DestinationColumn);
                    }
                }

                try
                {
                    if (truncateDestTable)
                        Truncate(bulk.DestinationTableName);

                    bulk.WriteToServer(reader);
                    var rowsCopied = bulk.GetRowsCopied();

                    OnImportCompleted(new DataImportCompleteArgs() { TotalRowsImported = rowsCopied, TableName = destTable });
                }
                catch (Exception ex)
                {
                    OnError(new DataImportErrorAgrs() { TableName = destTable, Error = ex, ImportStarted = importStart, ImportCompleted = importStop });
                }
            }
        }

        public void ImportDataTable(DataTable table, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            ImportDataTableInternal(table, destTable, truncateDestTable, columnMapping);
        }

        public void ImportDataTable(DataTable table, string destTable, bool truncateDestTable)
        {
            ImportDataTableInternal(table, destTable, truncateDestTable, null);
        }

        protected virtual void ImportDataTableInternal(DataTable table, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            OnImportStarting(new DataImportStartingArgs() { TableName = destTable });

            using (var bulk = new SqlBulkCopy(cs))
            {
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = destTable;

                if (columnMapping != null)
                {
                    foreach (var col in columnMapping)
                    {
                        bulk.ColumnMappings.Add(col.SourceColumn, col.DestinationColumn);
                    }
                }

                try
                {
                    if (truncateDestTable)
                        Truncate(bulk.DestinationTableName);

                    bulk.WriteToServer(table);

                    OnImportCompleted(new DataImportCompleteArgs() { TotalRowsImported = table.Rows.Count, TableName = destTable });
                }
                catch (Exception ex)
                {
                    OnError(new DataImportErrorAgrs() { TableName = destTable, Error = ex, ImportStarted = importStart, ImportCompleted = DateTime.Now });
                }
            }
        }

        public void ImportDataRow(DataRow[] rows, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            ImportDataRowInternal(rows, destTable, truncateDestTable, columnMapping);
        }
        public void ImportDataRow(DataRow[] rows, string destTable, bool truncateDestTable)
        {
            ImportDataRowInternal(rows, destTable, truncateDestTable, null);
        }

        protected virtual void ImportDataRowInternal(DataRow[] rows, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            OnImportStarting(new DataImportStartingArgs() { TableName = destTable });

            using (var bulk = new SqlBulkCopy(cs))
            {
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = destTable;

                if (columnMapping != null)
                {
                    foreach (var col in columnMapping)
                    {
                        bulk.ColumnMappings.Add(col.SourceColumn, col.DestinationColumn);
                    }
                }

                try
                {
                    if (truncateDestTable)
                        Truncate(bulk.DestinationTableName);

                    bulk.WriteToServer(rows);

                    OnImportCompleted(new DataImportCompleteArgs() { TotalRowsImported = rows.Count(), TableName = destTable });
                }
                catch (Exception ex)
                {
                    OnError(new DataImportErrorAgrs() { TableName = destTable, Error = ex, ImportStarted = importStart, ImportCompleted = DateTime.Now });
                }
            }
        }

        public void ImportDataRow(DbConnection connection, DbCommand command, string destTable, int batchSize)
        {
            ImportDataRowInternal(connection, command, destTable, batchSize);
        }

        protected void ImportDataRowInternal(DbConnection connection, DbCommand command, string destTable, int batchSize)
        {
            command.Connection = connection;
            command.CommandTimeout = 3600;
            connection.Open();

            var t = new DataTable();

            using (var cnn = new SqlConnection(cs))
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = $"select * from {destTable} where 1=0";
                cnn.Open();
                t.Load(cmd.ExecuteReader());
            }


            using (var reader = command.ExecuteReader())
            {
                var rows = new List<DataRow>(batchSize);

                OnImportStarting(new DataImportStartingArgs() { TableName = destTable });
                var truncateDestTable = true;
                var runningCount = 0;

                while (reader.Read())
                {
                    runningCount += 1;
                    var row = t.NewRow();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        try
                        {
                            var data = reader[reader.GetName(i)];

                            //weird infolease char that violates max length when populating datatable for import
                            row[reader.GetName(i)] = data.ToString().Contains("\u009d") ? "" : data;
                        }
                        catch (Exception ex)
                        {
                            OnDataError(new DataImportDataErrorArgs() { TableName = destTable, Error = ex, DataRowItemArray = row.ItemArray });

                            //OnError(new DataImportErrorAgrs() { TableName = destTable, Error = ex, ImportStarted = importStart, ImportCompleted = DateTime.Now });
                        }

                    }

                    rows.Add(row);


                    if (rows.Count() == batchSize)
                    {
                        ImportDataRow2(destTable, rows.ToArray(), truncateDestTable, runningCount);
                        rows.Clear();
                        t.Rows.Clear();
                        t.Clear();
                        truncateDestTable = false;
                    }
                }

                if (rows.Any())
                {
                    ImportDataRow2(destTable, rows.ToArray(), truncateDestTable, runningCount);
                    rows.Clear();
                    t.Rows.Clear();
                    t.Clear();
                }

                OnImportCompleted(new DataImportCompleteArgs() { TotalRowsImported = CountRows(destTable), TableName = destTable });
            }

        }

        private void ImportDataRow2(string destTable, DataRow[] rows, bool truncateDestTable, int totalRows)
        {
            using (var bulk = new SqlBulkCopy(cs))
            {
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = destTable;

                try
                {
                    if (truncateDestTable)
                        Truncate(bulk.DestinationTableName);

                    bulk.WriteToServer(rows);
                    OnImportProgress(new DataImportProgressArgs() { TableName = destTable, RowsImported = rows.Length, TotalRowsImported = totalRows });

                }
                catch (Exception ex)
                {
                    OnError(new DataImportErrorAgrs() { TableName = destTable, Error = ex, ImportStarted = importStart, ImportCompleted = importStop });
                }
            }
        }

        public void MergeDataTable(DataTable table, string destTable, string tempTable, params string[] keys)
        {
            using (var cnn = new SqlConnection(cs))
            {
                cnn.Open();

                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = $"IF OBJECT_ID('tempdb..#'{tempTable}') IS NOT NULL DROP TABLE #{tempTable}";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT * into #{tempTable} FROM {destTable} WHERE 1=0";
                    cmd.ExecuteNonQuery();
                }

                using (var bulk = new SqlBulkCopy(cnn))
                {
                    bulk.BulkCopyTimeout = 0;
                    bulk.DestinationTableName = tempTable;
                    bulk.WriteToServer(table);

                }

                var columns = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName);

                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = GetUpdateSqlForMerge(columns, tempTable, destTable, keys);
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = GetInsertSqlForMerge(tempTable, destTable, keys);
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = $"DROP TABLE #{tempTable}";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private string GetUpdateSqlForMerge(IEnumerable<string> columns, string sourceTable, string targetTable, params string[] keys)
        {
            var sql = $@"update {targetTable}
                        set
                            <columns>
                        from {sourceTable} s
                            join {targetTable} t on <keys>";

            var setcols = columns.Except(keys).Select(c => $"{c} = s.{c}");
            var onkeys = keys.Select(c => $"s.{c} = t.{c}");

            sql = sql.Replace("<columns>", string.Join(",", setcols));
            sql = sql.Replace("<keys>", string.Join(" AND ", onkeys));

            return sql;
        }

        private string GetInsertSqlForMerge(string sourceTable, string targetTable, params string[] keys)
        {
            var sql = $@"insert into {targetTable}
                        select * from {sourceTable} s
                            left join {targetTable} t on <keys>
                        where t.<anykeycol> is null";

            var onkeys = keys.Select(c => $"s.{c} = t.{c}");

            sql = sql.Replace("<keys>", string.Join(" AND ", onkeys));
            sql = sql.Replace("<anykeycol>", keys.First());

            return sql;
        }

        protected void Truncate(string tableName)
        {
            Execute($"TRUNCATE TABLE {tableName}");
        }

        protected int CountRows(string tablename)
        {
            using (var cnn = new SqlConnection(cs))
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = $"SELECT COUNT (*) FROM {tablename}";
                try
                {
                    cnn.Open();
                    var rows = cmd.ExecuteScalar();
                    return (int)rows;
                }
                catch (Exception ex)
                {
                    //OnError(ex);
                    return -1;
                }
            }
        }

        protected void Execute(string sql)
        {
            using (var cnn = new SqlConnection(cs))
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = sql;
                try
                {
                    cnn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    //OnError(ex);
                }
            }

        }
    }

    public static class SqlDataImportExtensions
    {
        public static int GetRowsCopied(this SqlBulkCopy bulk)
        {
            var rowsCopiedField = typeof(SqlBulkCopy).GetField("_rowsCopied", BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
            return (int)rowsCopiedField.GetValue(bulk);

        }
    }
}
