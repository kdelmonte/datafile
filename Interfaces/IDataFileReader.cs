using System;
using System.Data;
using DataFile.Models;

namespace DataFile.Interfaces
{
     interface IDataFileReader: IDataReader
     {
         string Path { get; set; }
         DataFileLayout Layout { get; set; }
         void Open();
         T GetValue<T>(int columnIndex);
         object[] GetValues();
    }
}
