using System.Collections.Generic;
using System.IO;

namespace DataFile
{
    public class BatchReader
    {
        private readonly long _maximumBatchSize;
        public List<string> Columns = new List<string>();
        public bool Complete;
        public string FilePath;
        public List<string> Rows = new List<string>();
        private int _rowsProcessed;

        public BatchReader(string filePath, long maxBatchSize)
        {
            FilePath = filePath;
            _maximumBatchSize = maxBatchSize;
            GetColumns();
        }

        public BatchReader(string filePath)
        {
            FilePath = filePath;
            GetNextBatchOfRows();
            GetColumns();
        }

        public void GetNextBatchOfRows()
        {
            Rows.Clear();
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(FilePath);
                reader.ReadLine(); //Skip to second row
                var nextLine = reader.ReadLine();
                for (var k = 0; k < _rowsProcessed; k++)
                {
                    reader.ReadLine();
                }

                if (_maximumBatchSize > 0)
                {
                    long limit = 0;
                    while (limit < _maximumBatchSize)
                    {
                        Rows.Add(nextLine);
                        nextLine = reader.ReadLine();
                        limit++;
                        _rowsProcessed++;
                        if (nextLine == null)
                        {
                            _rowsProcessed = 0;
                            Complete = true;
                            limit = _maximumBatchSize;
                        }
                    }
                }
                else
                {
                    while (nextLine != null)
                    {
                        Rows.Add(nextLine);
                        nextLine = reader.ReadLine();
                        _rowsProcessed++;
                        if (nextLine == null)
                        {
                            _rowsProcessed = 0;
                            Complete = true;
                        }
                    }
                }
            }
            finally
            {
                if (reader != null) reader.Close();
            }
        }

        private void GetColumns()
        {
            var reader = new StreamReader(FilePath);
            try
            {
                var nextLine = reader.ReadLine();

                if (nextLine != null)
                {
                    var columns = nextLine.Split(',');
                    foreach (var c in columns)
                    {
                        Columns.Add(c);
                    }
                }
                reader.Close();
            }
            finally
            {
                reader.Close();
            }
        }
    }
}