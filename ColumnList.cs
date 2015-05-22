using System.Collections.ObjectModel;
using System.Collections.Specialized;

namespace DataFile
{
    public sealed class ColumnList : ObservableCollection<Column>
    {
        public ColumnList()
        {
        }

        public ColumnList(NotifyCollectionChangedEventHandler onChange)
        {
            CollectionChanged += onChange;
        }
    }
}