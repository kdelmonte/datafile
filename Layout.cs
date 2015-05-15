using Newtonsoft.Json;

namespace Fable
{
    public class Layout
    {
        public ColumnList Columns = new ColumnList();
        public bool HasColumnHeaders;
        public string Name;

        public Layout()
        {
        }

        public Layout(string json)
        {
            var layout = JsonConvert.DeserializeObject<Layout>(json);
            if (layout == null) return;
            var l = layout.GetType();
            var properties = l.GetFields();
            foreach (var pi in properties)
            {
                pi.SetValue(this, pi.GetValue(layout));
            }
        }

        public Layout(string name, FableInfo leadFileInfo)
        {
            Name = name;
            Columns = leadFileInfo.Columns;
        }
    }
}