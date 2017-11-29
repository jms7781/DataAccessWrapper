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

namespace DAL
{
    /// <summary>
    /// Provides methods for importing data into a SqlServer database using SqlBulkCopy
    /// </summary>
    public class SqlDataImport
    {
        private string cs;

        public event EventHandler<Exception> Error;
        public event EventHandler<TableInfo> ImportStarting;
        public event EventHandler<DataExportArgs> ImportCompleted;
        public event EventHandler<DataExportArgs> ImportProgress;

        public int DataReaderRowsImported { get; private set; }

        protected DataExportArgs args;
        private Stopwatch timer;
        //private int rowTotal;
        

        public SqlDataImport(string connectionString)
        {
            cs = connectionString;
            args = new DataExportArgs();
            timer = new Stopwatch();
        }

        public SqlDataImport(string server, string database)
        {
            var cb = new SqlConnectionStringBuilder
            {
                DataSource = server,
                InitialCatalog = database,
                IntegratedSecurity = true
            };
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
        private void ImportDataReaderInternal(DbDataReader reader, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping, int batchSize)
        {
            using (var bulk = new SqlBulkCopy(cs))
            {
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = destTable;
                bulk.NotifyAfter = batchSize;
                bulk.SqlRowsCopied += (s, e) =>
                {
                    //Console.WriteLine(e.RowsCopied);
                    args.RowsImported = (int)e.RowsCopied;
                    OnImportProgress(args);
                    
                };

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
                    args.RowsImported = bulk.GetRowsCopied();


                    //args.RowsImported = bulk.GetRowsCopied();
                    //OnImportCompleted();
                }
                catch (Exception ex)
                {
                    OnError(ex);
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

        private void ImportDataTableInternal(DataTable table, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
            args.Table = new TableInfo { DestTableName = destTable, SourceTableName = destTable };
            OnImportStarting(args.Table);

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

                    args.RowsImported = table.Rows.Count;
                    OnImportCompleted(args);
                }
                catch (Exception ex)
                {
                    OnError(ex);
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

        private void ImportDataRowInternal(DataRow[] rows, string destTable, bool truncateDestTable, IEnumerable<SqlBulkCopyColumnMapping> columnMapping)
        {
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
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
            }
        }

        private void Truncate(string tableName)
        {
            Execute($"TRUNCATE TABLE {tableName}");
        }

        private int CountRows(string tablename)
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
                    OnError(ex);
                    return -1;
                }
            }
        }

        private void Execute(string sql)
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
                    OnError(ex);
                }
            }
        }

        protected virtual void OnError(Exception e)
        {
            Error?.Invoke(this, e);
        }

        protected virtual void OnImportStarting(TableInfo e)
        {
            //rowTotal = 0;

            timer.Restart();
            args.RowsImported = 0;
            args.ImportStart = DateTime.Now;
            ImportStarting?.Invoke(this, e);
        }

        protected virtual void OnImportProgress(DataExportArgs e)
        {
            ImportProgress?.Invoke(this, e);
        }

        protected virtual void OnImportCompleted(DataExportArgs e)
        {
            timer.Stop();
            e.ImportStop = DateTime.Now;
            e.Duration = timer.Elapsed;

            //args.RowsImported = CountRows(args.Table.DestTableName);
            
            //args.Message = $"export complete: {args.Duration.ToString()}";
            ImportCompleted?.Invoke(this, args);
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
