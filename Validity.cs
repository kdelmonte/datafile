using System.Collections.Generic;

namespace Fable
{
    public class Validity
    {
        public List<string> Errors = new List<string>();
        public List<string> Warnings = new List<string>();
        private bool valid = true;

        public bool Valid
        {
            get { return valid; }
        }

        public void AddError(string error)
        {
            if (!Errors.Contains(error))
            {
                Errors.Add(error);
            }
            valid = Errors.Count == 0;
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