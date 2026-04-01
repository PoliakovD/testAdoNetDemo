using System.Data.Common;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using Npgsql;

namespace ADO.NET.DAL;

public static class DbConnectionFactory
{
    public static DbConnection GetPostgreSqlConnection()
    {
        const string ConnectionString =
            "Host=localhost:5437;Username=postgres;Password=1234;Database=bbq_db;Search path=test_schema";
        return new NpgsqlConnection(ConnectionString);
    }

    public static DbConnection GetSqLiteConnection()
    {
        const string dbName = "bbq_db.db";

        const string ConnectionString =
            $"Data Source = {dbName}";
        try
        {
           
            if (!File.Exists(dbName))
            {
                var connection = new SqliteConnection(ConnectionString);

                connection.Open();
                Console.WriteLine(File.Exists(dbName));
                
                Console.WriteLine("Creating SQLite database...");

                const string creationCommand =
                    """
                    CREATE TABLE table_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    is_driver BOOLEAN DEFAULT FALSE
                    );

                    CREATE TABLE table_products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_name TEXT NOT NULL,
                    quantity TEXT,
                    price NUMERIC DEFAULT 0 CHECK (price >= 0),
                    is_purchased BOOLEAN DEFAULT FALSE,
                    user_id INTEGER,
                    FOREIGN KEY (user_id) REFERENCES table_users(id) ON DELETE SET NULL
                    );
                    -- 1. Добавляем компанию друзей (table_User)
                    INSERT INTO table_users (name, is_driver) VALUES
                    ('Даня', TRUE),    -- Организатор и водитель
                    ('Андрей', FALSE),
                    ('Марина', FALSE),
                    ('Олег', TRUE),    -- Второй водитель
                    ('Софья', FALSE);

                    -- 2. Заполняем список продуктов (table_products)
                    -- Обратите внимание: часть продуктов "куплена" (is_purchased = true), часть - нет.
                    INSERT INTO table_products (item_name, quantity, price, is_purchased, user_id) VALUES
                    ('Шейка свиная', '5 кг', 3500.00, TRUE, 1),   -- Купил Даня
                    ('Угли березовые', '2 упаковки', 600.00, TRUE, 1), -- Купил Даня
                    ('Помидоры и огурцы', '3 кг', 800.00, TRUE, 2), -- Купил Андрей
                    ('Соус шашлычный', '2 шт', 0.00, FALSE, 3),    -- Марина еще не купила
                    ('Лаваш свежий', '5 шт', 0.00, FALSE, 3),      -- Марина еще не купила
                    ('Вода без газа', '10 л', 450.00, TRUE, 4),    -- Купил Олег
                    ('Маршмэллоу', '2 пачки', 0.00, FALSE, 5),     -- Софья еще не купила
                    ('Секретный маринад', '1 банка', 0.00, FALSE, NULL); -- Никто не назначен! (тест для LEFT JOIN)
                    """;
                var command = new SqliteCommand(creationCommand, connection);
                command.ExecuteNonQuery();
                connection.Close();
                return connection;
            }
            else
            {
                return new SqliteConnection(ConnectionString);
                
            }

            
            
        }
        catch (Exception ex)
        {
            Console.WriteLine("Ошибка:" + ex.Message);
        }

        return null;
    }

    public static DbConnection GetMySqlConnection()
    {
        var connectionString =
            "server=localhost;Port=3306;user id=mysql;Password=1234;database=bbq_db;";

        return new MySqlConnection(connectionString);

    }
}