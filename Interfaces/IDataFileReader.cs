using System;
using System.Data;
using DataFile.Models;

namespace DataFile.Interfaces
{
     interface IDataFileReader: IDataReader, IDataFileRecord
    {
         string Path { get; set; }
         void Open();
    }
}
