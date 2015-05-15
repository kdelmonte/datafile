namespace Fable
{
    public class ColumnValueFrequency
    {
        public int Count;
        public double Percentage;
        public string Value;

        public ColumnValueFrequency(string value, int count, double percentage)
        {
            Value = value;
            Count = count;
            Percentage = percentage;
        }

        public ColumnValueFrequency(string value, int count)
        {
            Value = value;
            Count = count;
        }

        public ColumnValueFrequency()
        {
        }
    }
}