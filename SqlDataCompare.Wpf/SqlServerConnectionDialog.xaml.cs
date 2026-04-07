using System.Windows;
using Microsoft.Data.SqlClient;

namespace SqlDataCompare.Wpf;

public partial class SqlServerConnectionDialog : Window
{
    public string? ConnectionString { get; private set; }

    public SqlServerConnectionDialog(string? initialConnectionString)
    {
        InitializeComponent();
        AuthWindows.Checked += (_, _) => UpdateSqlAuthEnabled();
        AuthSql.Checked += (_, _) => UpdateSqlAuthEnabled();

        if (string.IsNullOrWhiteSpace(initialConnectionString))
        {
            UpdateSqlAuthEnabled();
            return;
        }

        try
        {
            var b = new SqlConnectionStringBuilder(initialConnectionString);
            ServerText.Text = b.DataSource ?? "";
            DatabaseText.Text = b.InitialCatalog ?? "";
            EncryptCheck.IsChecked = b.Encrypt;
            TrustCertCheck.IsChecked = b.TrustServerCertificate;
            if (b.IntegratedSecurity)
            {
                AuthWindows.IsChecked = true;
            }
            else
            {
                AuthSql.IsChecked = true;
                UserText.Text = b.UserID ?? "";
                PasswordText.Password = b.Password ?? "";
            }
        }
        catch
        {
            /* show empty form */
        }

        UpdateSqlAuthEnabled();
    }

    private void UpdateSqlAuthEnabled()
    {
        var sql = AuthSql.IsChecked == true;
        UserText.IsEnabled = sql;
        PasswordText.IsEnabled = sql;
    }

    private void Ok_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrWhiteSpace(ServerText.Text))
        {
            System.Windows.MessageBox.Show(this, "Enter a server name.", Title, MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        try
        {
            var b = new SqlConnectionStringBuilder
            {
                DataSource = ServerText.Text.Trim(),
                Encrypt = EncryptCheck.IsChecked == true,
                TrustServerCertificate = TrustCertCheck.IsChecked == true,
            };

            var db = DatabaseText.Text.Trim();
            if (!string.IsNullOrEmpty(db))
                b.InitialCatalog = db;

            if (AuthWindows.IsChecked == true)
            {
                b.IntegratedSecurity = true;
            }
            else
            {
                b.IntegratedSecurity = false;
                b.UserID = UserText.Text.Trim();
                b.Password = PasswordText.Password;
                if (!string.IsNullOrEmpty(PasswordText.Password))
                    b.PersistSecurityInfo = true;
            }

            ConnectionString = b.ConnectionString;
            DialogResult = true;
        }
        catch (Exception ex)
        {
            System.Windows.MessageBox.Show(this, ex.Message, Title, MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) =>
        DialogResult = false;

    public static bool TryShow(Window? owner, string? initialConnectionString, out string connectionString)
    {
        connectionString = "";
        var dlg = new SqlServerConnectionDialog(initialConnectionString);
        if (owner is not null)
            dlg.Owner = owner;
        if (dlg.ShowDialog() != true)
            return false;
        connectionString = dlg.ConnectionString ?? "";
        return !string.IsNullOrWhiteSpace(connectionString);
    }
}
