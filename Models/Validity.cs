using System.Collections.Generic;
using System.Linq;

namespace DataFile.Models
{
    public class Validity
    {
        public List<string> Errors = new List<string>();
        public List<string> Warnings = new List<string>();

        public bool Valid
        {
            get { return !Errors.Any(); }
        }

        public bool HasWarnings
        {
            get { return Warnings.Any(); }
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