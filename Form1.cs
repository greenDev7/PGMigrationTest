using Npgsql;
using Npgsql.Schema;
using System;
using System.Collections.ObjectModel;
using System.Windows.Forms;

namespace PGMigrationTest
{
    public partial class Form1 : Form
    {
        // Запрос для получения данных из ПРОДовской БД
        string selectDataFromProdDB = "SELECT * FROM prod_schema.prod_table order by id limit 100000;";
        // Запрос для получения данных из тестовой БД
        // Если названия БД/схемы на проде и локалке совпадают, 
        // то можно оставить только один из них
        string selectDataFromLocalDB = "SELECT * FROM local_schema.local_table;";

        public Form1()
        {
            InitializeComponent();
        }

        private void migrateDataButton_Click(object sender, EventArgs e)
        {
            using (var prodConnection = GetNpgsqlConnection(
                hostTextBox.Text,
                int.Parse(portTextBox.Text),
                userNameTextBox.Text, 
                passwordTextBox.Text,
                dataBaseTextBox.Text))
            {
                prodConnection.Open();

                using (var localConnection = GetNpgsqlConnection("127.0.0.1", 5432, "postgres", "12345", "local_db"))
                {
                    localConnection.Open();

                    NpgsqlDataAdapter npgsqlDataAdapter = new NpgsqlDataAdapter(selectDataFromLocalDB, localConnection);
                    NpgsqlCommandBuilder npgsqlCommandBuilder = new NpgsqlCommandBuilder(npgsqlDataAdapter);

                    using (NpgsqlCommand npgsqlCommand = new NpgsqlCommand(selectDataFromProdDB, prodConnection))
                    {
                        using (NpgsqlDataReader reader = npgsqlCommand.ExecuteReader())
                        {
                            // Получаем столбцы, их типы и еще много чего для дальнейших действий
                            ReadOnlyCollection<NpgsqlDbColumn> dbColumns = reader.GetColumnSchema(); 
                            while (reader.Read())
                            {
                                object[] values = new object[dbColumns.Count];
                                reader.GetValues(values);
                                // Метод GetInsertCommand вернет нам шаблон INSERT запроса вместе с параметрами, избавив
                                // нас от мучений динамического формирования параметров запроса
                                InsertPrepared(npgsqlCommandBuilder.GetInsertCommand(true), values, dbColumns);                                
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Возвращает экземпляр подключения
        /// </summary>
        /// <param name="host">хост</param>
        /// <param name="port">порт</param>
        /// <param name="username">логин</param>
        /// <param name="password">пароль</param>
        /// <param name="database">название БД</param>
        /// <param name="pooling">пуллинг</param>
        /// <returns></returns>
        private NpgsqlConnection GetNpgsqlConnection(string host, int port, string username, string password, string database, bool pooling = false)
        {
            NpgsqlConnectionStringBuilder npgsqlConnectionStringBuilder = new NpgsqlConnectionStringBuilder()
            {
                Host = host,
                Port = port,
                Username = username,
                Password = password,
                Database = database,
                Pooling = pooling
            };

            return new NpgsqlConnection(npgsqlConnectionStringBuilder.ConnectionString);
        }                

        private void InsertPrepared(NpgsqlCommand insertCommand, object[] values, ReadOnlyCollection<NpgsqlDbColumn> dbColumns)
        {
            int i = 0;
            foreach (NpgsqlDbColumn dbColumn in dbColumns)
            {
                string parameterName = $"@{dbColumn.ColumnName}";
                insertCommand.Parameters[parameterName].NpgsqlDbType = (NpgsqlTypes.NpgsqlDbType)dbColumn.NpgsqlDbType;
                insertCommand.Parameters[parameterName].Value = values[i];
                i++;
            }

            insertCommand.Prepare();
            insertCommand.ExecuteNonQuery();
        }
    }
}
