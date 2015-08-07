using System;
using System.Data;
using DataFile.Models;

namespace DataFile.Interfaces
{
     interface IDataFileRecord : IDataRecord
     {
         DataFileLayout Layout { get;}
         T GetValue<T>(int columnIndex);
         object[] GetValues();
    }
}
