using System;
using System.IO;

namespace DataFile
{
    public static class Utilities
    {
        public static string BytesToReadableSize(long length)
        {
            const int byteConversion = 1024;
            if (length >= Math.Pow(byteConversion, 3)) //GB Range
            {
                return string.Concat(Math.Round(length/Math.Pow(byteConversion, 3), 2), " GB");
            }
            if (length >= Math.Pow(byteConversion, 2)) //MB Range
            {
                return string.Concat(Math.Round(length/Math.Pow(byteConversion, 2), 2), " MB");
            }
            if (length >= byteConversion) //KB Range
            {
                return string.Concat(Math.Round((double) length/byteConversion, 2), " KB");
            }
            return string.Concat(length, " Bytes");
        }

        public static float CalculateFolderSize(DirectoryInfo folder)
        {
            var folderSize = 0.0f;
            try
            {
                //Checks if the path is valid or not
                if (!folder.Exists)
                {
                    return folderSize;
                }
                try
                {
                    foreach (var file in folder.GetFiles())
                    {
                        if (file.Exists)
                        {
                            folderSize += file.Length;
                        }
                    }

                    foreach (var dir in folder.GetDirectories())
                    {
                        folderSize += CalculateFolderSize(dir);
                    }
                }
                catch (NotSupportedException e)
                {
                    Console.WriteLine("Unable to calculate folder size: {0}", e.Message);
                }
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine("Unable to calculate folder size: {0}", e.Message);
            }
            return folderSize;
        }
    }
}