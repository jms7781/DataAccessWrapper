using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DataAccessWrapper
{
    public class DataConnection<TConnection> where TConnection : DbConnection, new()
    {
        private string connectionString;

        public DataConnection(string connectionString)
        {
            this.connectionString = connectionString;
            this.Connection = NewConnection();
        }

        public DbConnection Connection { get; set; }

        public DbProviderFactory Factory { get { return DbProviderFactories.GetFactory(this.Connection); } }

        public DataTable GetSchema()
        {
            using (var cnn = NewConnection())
            {
                cnn.Open();
                return cnn.GetSchema();
            }
        }

        public DataTable GetSchema(string collectionName)
        {
            using (var cnn = NewConnection())
            {
                cnn.Open();
                return cnn.GetSchema(collectionName);
            }
        }

        public DataTable QueryDataTable(string sql)
        {
            return QueryDataTableInternal(CreateCommand(sql));
        }

        public DataTable QueryDataTable(DbCommand command)
        {
            return QueryDataTableInternal(command);
        }

        public DataTable QueryDataTable(string sql, params DbParameter[] parameters)
        {
            return QueryDataTableInternal(CreateCommand(sql, parameters));
        }

        private DataTable QueryDataTableInternal(DbCommand command)
        {
            var t = new DataTable();
            t.Load(QueryDataReaderInternal(command));
            return t;
        }

        public DataSet QueryMultipleDataSet(string sql)
        {
            return QueryMultipleDataSetInternal(CreateCommand(sql));
        }

        public DataSet QueryMultipleDataSet(string sql, params DbParameter[] parameters)
        {
            return QueryMultipleDataSetInternal(CreateCommand(sql, parameters));
        }

        public DataSet QueryMultipleDataSet(DbCommand command)
        {
            return QueryMultipleDataSetInternal(command);
        }

        private DataSet QueryMultipleDataSetInternal(DbCommand command)
        {
            var da = Factory.CreateDataAdapter();
            da.SelectCommand = command;
            var ds = new DataSet();
            da.Fill(ds);

            return ds;
        }

        public IEnumerable<DataTable> QueryMultipleDataTable(string sql)
        {
            return QueryMultipleDataTableInternal(CreateCommand(sql));
        }

        public IEnumerable<DataTable> QueryMultipleDataTable(string sql, params DbParameter[] parameters)
        {
            return QueryMultipleDataTableInternal(CreateCommand(sql, parameters));
        }

        public IEnumerable<DataTable> QueryMultipleDataTable(DbCommand command)
        {
            return QueryMultipleDataTableInternal(command);
        }

        public IEnumerable<DataTable> QueryMultipleDataTable(IEnumerable<DbCommand> commands)
        {
            foreach (var command in commands)
            {
                foreach (var table in QueryMultipleDataTableInternal(command))
                {
                    yield return table;
                }
            }
        }

        private IEnumerable<DataTable> QueryMultipleDataTableInternal(DbCommand command)
        {
            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();

                var reader = command.ExecuteReader();

                while (reader.HasRows)
                {
                    var t = new DataTable();
                    t.Load(reader);

                    yield return t;
                    reader.NextResult();
                }
            }
        }


        public DbDataReader QueryDataReader(string sql)
        {
            return QueryDataReaderInternal(CreateCommand(sql));
        }

        public DbDataReader QueryDataReader(DbCommand command)
        {
            return QueryDataReaderInternal(command);
        }

        public DbDataReader QueryDataReader(string sql, params DbParameter[] parameters)
        {
            return QueryDataReaderInternal(CreateCommand(sql, parameters));
        }

        private DbDataReader QueryDataReaderInternal(DbCommand command)
        {
            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();

                var reader = command.ExecuteReader();
                OnCommandExecuted(command);
                return reader;
            }
        }

        public DbDataAdapter QueryDataAdapter(string sql)
        {
            return QueryDataAdapterInternal(CreateCommand(sql));
        }

        public DbDataAdapter QueryDataAdapter(DbCommand command)
        {
            return QueryDataAdapterInternal(command);
        }

        public DbDataAdapter QueryDataAdapter(string sql, params DbParameter[] parameters)
        {
            return QueryDataAdapterInternal(CreateCommand(sql, parameters));
        }

        private DbDataAdapter QueryDataAdapterInternal(DbCommand selectCommand)
        {
            var adapter = Factory.CreateDataAdapter();
            var cb = Factory.CreateCommandBuilder();
            cb.DataAdapter = adapter;

            selectCommand.Connection = NewConnection();

            adapter.SelectCommand = selectCommand;
            adapter.InsertCommand = cb.GetInsertCommand();
            adapter.UpdateCommand = cb.GetUpdateCommand();
            adapter.DeleteCommand = cb.GetDeleteCommand();

            return adapter;
        }

        public IEnumerable<IDataRecord> QueryDataRecord(string sql)
        {
            return QueryDataRecordInternal(CreateCommand(sql));
        }

        public IEnumerable<IDataRecord> QueryDataRecord(DbCommand command)
        {
            return QueryDataRecordInternal(command);
        }

        public IEnumerable<IDataRecord> QueryDataRecord(string sql, params DbParameter[] parameters)
        {
            return QueryDataRecordInternal(CreateCommand(sql, parameters));
        }

        private IEnumerable<IDataRecord> QueryDataRecordInternal(DbCommand command)
        {
            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();

                var reader = command.ExecuteReader();
                OnCommandExecuted(command);

                while (reader.Read())
                {
                    yield return reader;
                }
            }
            using (var reader = QueryDataReaderInternal(command))
            {

            }
        }

        public IEnumerable<string> QueryString(string sql)
        {
            return QueryStringInternal(CreateCommand(sql));
        }

        public IEnumerable<string> QueryString(DbCommand command)
        {
            return QueryStringInternal(command);
        }

        public IEnumerable<string> QueryString(string sql, params DbParameter[] parameters)
        {
            return QueryStringInternal(CreateCommand(sql, parameters));
        }

        private IEnumerable<string> QueryStringInternal(DbCommand command)
        {
            foreach (var reader in QueryDataRecordInternal(command))
            {
                yield return reader.IsDBNull(0) ? "" : reader[0].ToString();
            }
        }

        public IEnumerable<string[]> QueryStringArray(string sql)
        {
            return QueryStringArrayInternal(CreateCommand(sql));
        }

        public IEnumerable<string[]> QueryStringArray(DbCommand command)
        {
            return QueryStringArrayInternal(command);
        }

        public IEnumerable<string[]> QueryStringArray(string sql, params DbParameter[] parameters)
        {
            return QueryStringArrayInternal(CreateCommand(sql, parameters));
        }

        private IEnumerable<string[]> QueryStringArrayInternal(DbCommand command)
        {
            foreach (var reader in QueryDataRecordInternal(command))
            {
                string[] vals = new string[reader.FieldCount];

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    vals[i] = reader.IsDBNull(i) ? "" : reader[i].ToString();
                }

                yield return vals;
            }
        }

        public T QueryValue<T>(string sql)
        {
            return QueryValueInternal<T>(CreateCommand(sql));
        }

        public T QueryValue<T>(DbCommand command)
        {
            return QueryValueInternal<T>(command);
        }

        public T QueryValue<T>(string sql, params DbParameter[] parameters)
        {
            return QueryValueInternal<T>(CreateCommand(sql, parameters));
        }

        private T QueryValueInternal<T>(DbCommand command)
        {
            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();

                var result = command.ExecuteScalar();
                OnCommandExecuted(command);
                return (T)result;
            }
        }

        public int Execute(string sql)
        {
            return ExecuteInternal(CreateCommand(sql));
        }

        public int Execute(DbCommand command)
        {
            return ExecuteInternal(command);
        }

        public int Execute(string sql, params DbParameter[] parameters)
        {
            return ExecuteInternal(CreateCommand(sql, parameters));
        }

        private int ExecuteInternal(DbCommand command)
        {
            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();

                var result = command.ExecuteNonQuery();
                OnCommandExecuted(command);
                return result;
            }
        }

        public IEnumerable<DataRow[]> QueryDataRow(string sql)
        {
            return QueryDataRow(sql, 0);
        }

        public IEnumerable<DataRow[]> QueryDataRow(string sql, int batchSize)
        {
            return QueryDataRowInternal(CreateCommand(sql), batchSize);
        }

        public IEnumerable<DataRow[]> QueryDataRow(DbCommand command)
        {
            return QueryDataRowInternal(command, 0);
        }

        public IEnumerable<DataRow[]> QueryDataRow(DbCommand command, int batchSize)
        {
            return QueryDataRowInternal(command, batchSize);
        }

        public IEnumerable<DataRow[]> QueryDataRow(string sql, params DbParameter[] parameters)
        {
            return QueryDataRow(sql, 0, parameters);
        }

        public IEnumerable<DataRow[]> QueryDataRow(string sql, int batchSize, params DbParameter[] parameters)
        {
            return QueryDataRowInternal(CreateCommand(sql, parameters), batchSize);
        }

        private IEnumerable<DataRow[]> QueryDataRowInternal(DbCommand command, int batchSize)
        {
            var rows = new List<DataRow>();
            var schema = new DataTable();

            using (var cnn = NewConnection())
            using (command)
            {
                command.Connection = cnn;
                cnn.Open();
                schema.Load(command.ExecuteReader(CommandBehavior.SchemaOnly));

                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var row = schema.NewRow();

                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var colName = reader.GetName(i);
                                row[colName] = reader[colName];
                            }

                            rows.Add(row);
                            schema.Clear();     //memory leak if the datatable isn't cleared

                            if (rows.Count == batchSize)
                            {
                                yield return rows.ToArray();
                            }
                        }
                    }

                }
                yield return rows.ToArray();
            }
        }

        public void ExecuteCommands(List<DbCommand> commands)
        {
            using (var cnn = NewConnection())
            {
                cnn.Open();
                using (DbTransaction tran = cnn.BeginTransaction())
                {
                    try
                    {
                        foreach (var command in commands)
                        {
                            using (command)
                            {
                                command.Transaction = tran;
                                command.Connection = cnn;
                                command.ExecuteNonQuery();
                                OnCommandExecuted(command);
                            }
                        }
                        tran.Commit();
                    }
                    catch
                    {
                        tran.Rollback();
                        throw;
                    }
                }
            }
        }

        public void ExecuteStoredProcedure(string name)
        {
            using (var cnn = NewConnection())
            using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = name;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
                OnCommandExecuted(cmd);
            }
        }

        private TConnection NewConnection() => new TConnection() { ConnectionString = this.connectionString };

        public DbParameter CreateParameter(string parameterName, object value, DbType datatype)
        {
            var p = Factory.CreateParameter();
            p.ParameterName = parameterName;
            p.DbType = datatype;
            p.Value = value;

            return p;
        }

        public DbCommand CreateCommand(string sql)
        {
            return CreateCommand(sql, null);
        }

        public DbCommand CreateCommand(string sql, params DbParameter[] parameters)
        {
            var cmd = Factory.CreateCommand();
            cmd.CommandText = sql;

            if (parameters != null)
                cmd.Parameters.AddRange(parameters);

            return cmd;
        }

        private DataTable GetQuerySchema(DbCommand command)
        {
            var t = new DataTable();
            using (command)
            {
                t.Load(command.ExecuteReader(CommandBehavior.SchemaOnly));
            }

            return t;
        }

        public event EventHandler<DbCommand> CommandExecuted;

        private void OnCommandExecuted(DbCommand command)
        {
            CommandExecuted?.Invoke(this, command);
        }

        public event EventHandler<Exception> Error;

        private void OnError(Exception e)
        {
            Error?.Invoke(this, e);
        }
    }
}
