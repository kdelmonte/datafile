using System.Collections.ObjectModel;
using System.Collections.Specialized;

namespace DataFile.Models
{
    public sealed class DataFileColumnList : ObservableCollection<DataFileColumn>
    {
        public DataFileColumnList()
        {
        }

        public DataFileColumnList(NotifyCollectionChangedEventHandler onChange)
        {
            CollectionChanged += onChange;
        }
    }
}