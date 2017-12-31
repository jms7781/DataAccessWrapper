using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.SqlClient;
using DataAccessWrapper;
using System.Data.Common;
using System.Data;

namespace UnitTests
{
    [TestClass]
    public class SqlServer
    {
        private const string SELECT_SQL = @"select * from person.person";
        private const string sql_select_param = @"select * from person.person where LastName = @1";
        private const string cs = @"Server=xps13\sqlexpress;Database=AdventureWorks2014;Trusted_Connection=True;";

        private DataConnection<SqlConnection> DC()
        {
            return new DataConnection<SqlConnection>(cs);
        }

        [TestMethod]
        public void OpenSqlConnection()
        {
            DC().Connection.Open();
        }

        [TestMethod]
        public void GetSchema()
        {
            var t = DC().GetSchema();

            
        }

        [TestMethod]
        public void Execute()
        {
            DC().Execute(SELECT_SQL);
        }

        [TestMethod]
        public void Execute_OneParameter()
        {
            DC().Execute(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataTable()
        {
            DC().QueryDataTable(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataTable_OneParameter()
        {
            DC().QueryDataTable(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataReader()
        {
            DC().QueryDataReader(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataReader_OneParameter()
        {
            DC().QueryDataReader(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }


        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataRecord()
        {
            DC().QueryDataRecord(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataRecord_OneParameter()
        {
            DC().QueryDataRecord(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataRow()
        {
            DC().QueryDataRow(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataRow_OneParameter()
        {
            DC().QueryDataRow(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataAdapter()
        {
            DC().QueryDataAdapter(SELECT_SQL);
        }


        [TestMethod]
        [TestCategory("Query")]
        public void QueryDataAdapter_OneParameter()
        {
            DC().QueryDataAdapter(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryString()
        {
            DC().QueryString(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryString_OneParameter()
        {
            DC().QueryString(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryStringArray()
        {
            DC().QueryStringArray(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryStringArray_OneParameter()
        {
            DC().QueryStringArray(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryValue()
        {
            DC().QueryValue<object>(SELECT_SQL);
        }

        [TestMethod]
        [TestCategory("Query")]
        public void QueryValue_OneParameter()
        {
            DC().QueryValue<object>(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString));
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataRecords()
        {
            foreach(var row in DC().QueryDataRecord(SELECT_SQL))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataRecords_OneParameter()
        {
            foreach (var row in DC().QueryDataRecord(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString)))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataRows()
        {
            foreach (var row in DC().QueryDataRow(SELECT_SQL))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataRows_OneParameter()
        {
            foreach (var row in DC().QueryDataRow(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString)))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateStrings()
        {
            foreach (var row in DC().QueryString(SELECT_SQL))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateStrings_OneParameter()
        {
            foreach (var row in DC().QueryString(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString)))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateStringArray()
        {
            foreach (var row in DC().QueryStringArray(SELECT_SQL))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateStringArray_OneParameter()
        {
            foreach (var row in DC().QueryStringArray(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString)))
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataTableRows()
        {
            foreach (DataRow row in DC().QueryDataTable(SELECT_SQL).Rows)
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateDataTableRows_OneParameter()
        {
            foreach (DataRow row in DC().QueryDataTable(sql_select_param, DC().CreateParameter("@1", "Smith", System.Data.DbType.AnsiString)).Rows)
            {

            }
        }

        [TestMethod]
        [TestCategory("Iterations")]
        public void IterateMultipleDataTable()
        {
            foreach (var table in DC().QueryMultipleDataTable(SELECT_SQL + " go\n" + SELECT_SQL))
            {
                var x = table.Rows.Count;
            }
        }
    }
}
