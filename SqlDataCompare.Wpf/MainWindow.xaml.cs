using System.Windows;

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
}
