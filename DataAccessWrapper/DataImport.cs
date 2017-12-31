using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccessWrapper
{
    public abstract class DataImport
    {
        public event EventHandler<DataImportErrorAgrs> Error;
        public event EventHandler<DataImportDataErrorArgs> DataError;
        public event EventHandler<DataImportStartingArgs> ImportStarting;
        public event EventHandler<DataImportCompleteArgs> ImportCompleted;
        public event EventHandler<DataImportProgressArgs> ImportProgress;

        protected Stopwatch timer;
        protected int totalRows;
        protected DateTime importStart;
        protected DateTime importStop;
        protected DateTime lastImportProgress;

        protected DataImport()
        {
            timer = new Stopwatch();
        }

        protected virtual void OnError(DataImportErrorAgrs e)
        {
            Error?.Invoke(this, e);
        }

        protected virtual void OnDataError(DataImportDataErrorArgs e)
        {
            DataError?.Invoke(this, e);
        }

        protected virtual void OnImportStarting(DataImportStartingArgs e)
        {
            totalRows = 0;
            timer.Restart();
            importStart = DateTime.Now;

            ImportStarting?.Invoke(this, e);
        }

        protected virtual void OnImportProgress(DataImportProgressArgs e)
        {

            totalRows = e.RowsImported;

            e.TotalElapsed = DateTime.Now - importStart;
            e.Elapsed = lastImportProgress == DateTime.MinValue ? e.TotalElapsed : DateTime.Now - lastImportProgress;

            lastImportProgress = DateTime.Now;

            ImportProgress?.Invoke(this, e);
        }

        protected virtual void OnImportCompleted(DataImportCompleteArgs e)
        {
            timer.Stop();
            e.ImportStarted = importStart;
            e.ImportCompleted = DateTime.Now;
            e.TotalElapsed = timer.Elapsed;

            ImportCompleted?.Invoke(this, e);
        }
    }

    public class DataImportStartingArgs : EventArgs
    {
        public string TableName { get; set; }
        public DateTime ImportStarted { get; set; }
    }

    public class DataImportErrorAgrs : DataImportStartingArgs
    {
        public Exception Error { get; set; }
        public DateTime ImportCompleted { get; set; }
    }

    public class DataImportDataErrorArgs : DataImportErrorAgrs
    {
        public object[] DataRowItemArray { get; set; }
    }

    public class DataImportProgressArgs : DataImportStartingArgs
    {
        public TimeSpan TotalElapsed { get; set; }
        public int TotalRowsImported { get; set; }
        public int RowsImported { get; set; }

        public TimeSpan Elapsed { get; set; }
    }

    public class DataImportCompleteArgs : DataImportProgressArgs
    {
        public DateTime ImportCompleted { get; set; }
    }
}
