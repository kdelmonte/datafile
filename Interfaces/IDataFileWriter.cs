using System;
using System.Collections.Generic;
using System.IO;
using DataFile.Models;

namespace DataFile.Interfaces
{
    interface IDataFileWriter: IDisposable
    {
        Stream Stream { get; set; }
        DataFileLayout Layout { get; set; }

        void Write(IEnumerable<object> values);

        void Write(DataFileReader reader);

        void Open();

        void Close();
    }
}
