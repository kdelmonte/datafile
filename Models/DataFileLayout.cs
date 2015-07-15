using Newtonsoft.Json;

namespace DataFile.Models
{
    public class DataFileLayout
    {
        public DataFileColumnList Columns = new DataFileColumnList();
        public bool HasColumnHeaders;
        public string Name;

        public DataFileLayout()
        {
        }

        public DataFileLayout(string json)
        {
            var layout = JsonConvert.DeserializeObject<DataFileLayout>(json);
            if (layout == null) return;
            var l = layout.GetType();
            var properties = l.GetFields();
            foreach (var pi in properties)
            {
                pi.SetValue(this, pi.GetValue(layout));
            }
        }

        public DataFileLayout(string name, DataFileInfo leadFileInfo)
        {
            Name = name;
            Columns = leadFileInfo.Columns;
        }
    }
}