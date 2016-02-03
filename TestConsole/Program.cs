using System;
using System.IO;
using DataFile;
using DataFile.Models;
using DataFile.Models.Database.Adapters;
using TestConsole.Properties;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var start = DateTime.Now;
            var targetFile = new FileInfo(@"C:\Users\kelvin.delmonte\Desktop\Book1.xlsx");
            Console.WriteLine("Processing {0}", targetFile.FullName);
            var transactSqlAdapter = new TransactSqlAdapter(Settings.Default.ConnString)
            {
                CommandTimeout = 0
            };
            var layout = new DataFileLayout
            {
                Columns = new DataFileColumnList
                {
                    new DataFileColumn
                    {
                        Name = "Unique ID"
                    },
                    new DataFileColumn
                    {
                        Name = "First Name*",
                        Required = true
                    },
                    new DataFileColumn
                    {
                        Name = "Middle Name"
                    },
                    new DataFileColumn
                    {
                        Name = "Last Name*",
                        Required = true
                    },
                    new DataFileColumn
                    {
                        Name = "Email",
                        Pattern = @"^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"
                    },
                    new DataFileColumn
                    {
                        Name = "Username*",
                        Required = true
                    },
                    new DataFileColumn
                    {
                        Name = "Role*",
                        Required = true
                    },
                    new DataFileColumn
                    {
                        Name = "Location*",
                        Required = true
                    },
                    new DataFileColumn
                    {
                        Name = "Admin",
                        AllowedValues = new [] {"YES", "NO"},
                        AllowedValuesComparison = StringComparison.OrdinalIgnoreCase
                    },
                    new DataFileColumn
                    {
                        Name = "Super Admin"
                    },
                    new DataFileColumn
                    {
                        Name = "Group"
                    }
                }
            };

            // Test layout:
            // var dataFile = new DataFileInfo(targetFile.FullName, layout, transactSqlAdapter)
            using (var dataFile = new DataFileInfo(targetFile.FullName, transactSqlAdapter) { TableName = targetFile.Name })
            {
                dataFile.Validate();
                if (!dataFile.Validity.Valid)
                {
                    Console.WriteLine("FILE HAS ERRORS:");
                    foreach (var error in dataFile.Validity.Errors)
                    {
                        Console.WriteLine($"> {error}");
                    }
                    if (dataFile.Validity.HasInvalidValues)
                    {
                        Console.WriteLine("INVALID VALUES DETAIL:");
                        foreach (var invalidValue in dataFile.Validity.InvalidValues)
                        {
                            Console.WriteLine($"row {invalidValue.RowNumber}, column: {invalidValue.ColumnIndex}");
                            if (invalidValue.Error.DataType)
                            {
                                Console.WriteLine("Value does not match type");
                            }
                            if (invalidValue.Error.Required)
                            {
                                Console.WriteLine("Value cannot be empty");
                            }
                            if (invalidValue.Error.MinLength)
                            {
                                Console.WriteLine("Value is too short");
                            }
                            if (invalidValue.Error.MaxLength)
                            {
                                Console.WriteLine("Value is too long");
                            }
                            if (invalidValue.Error.AllowedValues)
                            {
                                Console.WriteLine("Value does not match any of the allowed values");
                            }
                            if (invalidValue.Error.Pattern)
                            {
                                Console.WriteLine("Value does not match pattern specified");
                            }
                        }
                    }
                }
                dataFile.BeginDatabaseSession();
            }
            var end = DateTime.Now;
            var timeElapsed = end - start;
            Console.WriteLine("Total time elapsed: {0} hour(s), {1} minute(s), {2} second(s)", timeElapsed.Hours,
                timeElapsed.Minutes, timeElapsed.Seconds);
            Console.Beep();
            Console.ReadKey();
        }
    }
}
