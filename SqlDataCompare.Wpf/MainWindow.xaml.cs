using System.Linq;
using System.Windows;
using System.Windows.Controls;

namespace SqlDataCompare.Wpf;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        DataContext = new MainViewModel();
    }

    private void Exit_Click(object sender, RoutedEventArgs e) =>
        System.Windows.Application.Current.Shutdown();

    private void CompareResultsGrid_OnSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (DataContext is MainViewModel vm && sender is DataGrid dg)
            vm.SetBulkSelectedCompareResults(dg.SelectedItems.Cast<CompareResultTableVm>());
    }

    private void RowDiffsGrid_OnSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (DataContext is MainViewModel vm && sender is DataGrid dg)
            vm.SetBulkSelectedRowDiffs(dg.SelectedItems.Cast<RowDiffSelectableVm>());
    }
}
