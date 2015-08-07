using System.Collections.Generic;
using System.Linq;

namespace DataFile.Models
{
    public class DataFileValidity
    {
        public List<DataFileValueValidity> InvalidValues { get; }
        public List<string> Errors { get; }
        public List<string> Warnings { get; }

        public bool Valid => !Errors.Any();

        public bool HasWarnings => Warnings.Any();

        public bool HasInvalidValues => InvalidValues.Any();

        public DataFileValidity()
        {
            Errors = new List<string>();
            Warnings = new List<string>();
            InvalidValues = new List<DataFileValueValidity>();
        }

        public void AddError(string error)
        {
            if (!Errors.Contains(error))
            {
                Errors.Add(error);
            }
        }

        public void AddWarning(string warning)
        {
            if (!Warnings.Contains(warning))
            {
                Warnings.Add(warning);
            }
        }
    }
}