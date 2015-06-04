using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models
{
    public enum DatabaseCommandMode
    {
        Select = 1,
        Delete = 2,
        Update = 3,
        Insert = 4,
        Alter = 6
    }
}
