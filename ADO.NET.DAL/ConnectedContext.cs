using System.Data;
using System.Data.Common;
using ADO.NET.DAL.Models;
using Npgsql;

namespace ADO.NET.DAL;

public class ConnectedContext(DbConnection connection)
{
    private readonly DbConnection _connection = connection;

    public IEnumerable<Product> GetAllProducts()
    {
        var result = new List<Product>();
        _connection.Open();
        // const string sql = """
        //                     SELECT table_products.id AS id, price, item_name, quantity, is_purchased, name
        //                     FROM table_products
        //                     INNER JOIN table_users ON  table_users.id = table_products.user_id
        //                    """;

        const string sql = """
                            SELECT table_products.id AS id, price, item_name, quantity, is_purchased, name
                            FROM table_products
                            LEFT JOIN table_users ON  table_users.id = table_products.user_id
                           """;

        var command = _connection.CreateCommand();

        command.CommandText = sql;

        var reader = command.ExecuteReader();

        // if (reader.HasRows)
        // {
        //     while (reader.Read())
        //     {
        //         var id = reader.GetInt32("id");
        //         var itemName = reader.GetString("item_name");
        //         var quantity = reader.GetString("quantity");
        //         var price = reader.GetDecimal("price");
        //         var isPurchased = reader.GetBoolean("is_purchased");
        //         
        //         
        //         //var userName = reader.GetString("name");
        //         var userName = reader.IsDBNull("name") ? "Еще никто не взялся!" : reader.GetString("name");
        //         
        //         result.Add(new Product()
        //         {
        //             Id = id,
        //             IsPurchased = isPurchased,
        //             UserName = userName,
        //             Name = itemName,
        //             Quantity = quantity,
        //             Price = price,
        //         });
        //     }
        // }

        if (reader.HasRows)
        {
            // 1. Узнаем номера колонок ОДИН РАЗ перед циклом
            int idPos = reader.GetOrdinal("id");
            int namePos = reader.GetOrdinal("item_name");
            int qtyPos = reader.GetOrdinal("quantity");
            int pricePos = reader.GetOrdinal("price");
            int purchasedPos = reader.GetOrdinal("is_purchased");
            int userPos = reader.GetOrdinal("name");

            while (reader.Read())
            {
                // 2. Внутри цикла работаем по индексам (это молниеносно)
                result.Add(new Product()
                {
                    Id = reader.GetInt32(idPos),
                    Name = reader.GetString(namePos),
                    Quantity = reader.GetString(qtyPos),
                    Price = reader.GetDecimal(pricePos),
                    IsPurchased = reader.GetBoolean(purchasedPos),

                    // Проверка на NULL тоже быстрее по индексу
                    UserName = reader.IsDBNull(userPos) ? "Еще никто не взялся!!!" : reader.GetString(userPos)
                });
            }
        }

        _connection.Close();
        return result;
    }

    public void InsertNewUser(User user)
    {
        _connection.Open();

        var command = _connection.CreateCommand();
        command.CommandText = $"""
                                 INSERT INTO table_users (name, is_driver)
                                 VALUES ('{user.Name}', {user.IsDriver})
                               """;
        command.ExecuteNonQuery();
        Console.WriteLine($"Пользователь {user.Name}, {user.IsDriver} добавлен");
        _connection.Close();
    }

    public void InsertNewUserWithParameters(User user)
    {
        _connection.Open();

        var command = _connection.CreateCommand();
        command.CommandText = """
                                INSERT INTO table_users (name, is_driver)
                                VALUES (@name, @is_driver)
                              """;

        var userNameParameter = command.CreateParameter();
        userNameParameter.ParameterName = "@name";
        userNameParameter.Value = user.Name;
        command.Parameters.Add(userNameParameter);


        var isDriverParameter = command.CreateParameter();
        isDriverParameter.ParameterName = "@is_driver";
        isDriverParameter.Value = user.IsDriver;
        command.Parameters.Add(isDriverParameter);


        command.ExecuteNonQuery();
        
        
        Console.WriteLine($"Пользователь {user.Name}, {user.IsDriver} добавлен");
        _connection.Close();
    }

    public void InsertNewUserWithParametersAdvanced(User user)
    {
        _connection.Open();

        var command = _connection.CreateCommand();
        command.CommandText = """
                                INSERT INTO table_users (name, is_driver)
                                VALUES (@name, @is_driver)
                              """;

        command.AddParameterWithValue("@name", user.Name);
        command.AddParameterWithValue("@is_driver", user.IsDriver);


        command.ExecuteNonQuery();
        Console.WriteLine($"Пользователь {user.Name}, {user.IsDriver} добавлен");
        _connection.Close();
    }

    public IEnumerable<User> GetAllUsers()
    {
        var result = new List<User>();
        _connection.Open();

        const string sql = """
                            SELECT  id, name, is_driver
                            FROM table_users;
                           """;

        var command = _connection.CreateCommand();

        command.CommandText = sql;

        var reader = command.ExecuteReader();

        if (reader.HasRows)
        {
            while (reader.Read())
            {
                var id = reader.GetInt32("id");
                var isDriver = reader.GetBoolean("is_driver");
                var name = reader.GetString("name");

                result.Add(new User()
                {
                    Id = id,
                    Name = name,
                    IsDriver = isDriver,
                });
            }
        }

        _connection.Close();
        return result;
    }

    public decimal GetAvgPrice()
    {
        decimal result = 0m;

        _connection.Open();

        var command = _connection.CreateCommand();

        command.CommandText = """
                                SELECT avg(price) FROM  table_products;
                              """;
        var rawResult = command.ExecuteScalar();
        if (rawResult != null && rawResult != DBNull.Value)
        {
            result = Convert.ToDecimal(rawResult);
        }

        _connection.Close();
        return result;
    }

    public decimal UpdatePrice(int id, decimal newPrice)
    {
        _connection.Open();

        using var command = _connection.CreateCommand();
        command.CommandText = "update_product_price";
        command.CommandType = CommandType.StoredProcedure;
        

        // command.AddParameterWithValue("p_id", id);
        // command.AddParameterWithValue("p_new_price", newPrice);

        // var idParam = command.CreateParameter();
        // idParam.ParameterName = "p_id";
        // idParam.Value = id;
        // idParam.Direction = ParameterDirection.Input; // По умолчанию Input
        // command.Parameters.Add(idParam);
        command.AddParameterWithValue("@p_id", id);


        // var priceParam = command.CreateParameter();
        // priceParam.ParameterName = "p_new_price";
        // priceParam.Value = newPrice;
        // command.Parameters.Add(priceParam);
        command.AddParameterWithValue("@p_new_price", newPrice);

        // var output = command.CreateParameter();
        // output.Direction = ParameterDirection.Output;
        // output.ParameterName = "p_old_price";
        // output.DbType =  DbType.Decimal;
        // command.Parameters.Add(output);
        command.AddParameterWithValue("@p_old_price", null, ParameterDirection.Output, DbType.Decimal);

        try
        {
            command.ExecuteNonQuery();
            // var outputOldPrice = (decimal)command.Parameters["p_old_price"].Value;
            var outputOldPrice = (decimal)command.Parameters["@p_old_price"].Value;
            return outputOldPrice;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при вызове процедуры: {ex.Message}");
        }
        finally
        {
            _connection.Close();
        }

        return 0;
    }

    //Пакетная обработка запросов
    public void InsertUsersBatch(List<User> users)
    {
        _connection.Open();

        try
        {
            using var command = _connection.CreateCommand() ;

            var sqlBuilder = new System.Text.StringBuilder();

            for (int i = 0; i < users.Count; i++)
            {
                // Генерируем уникальные имена параметров для каждой строки
                string nameParam = $"name{i}";
                string driverParam = $"isDriver{i}";

                // Добавляем SQL конструкцию (через точку с запятой для пакета)
                sqlBuilder.AppendLine($"""
                                       INSERT INTO table_users (name, is_driver) 
                                       VALUES (@{nameParam}, @{driverParam});
                                       """);

                // 2. Используем ВАШ метод расширения
                command.AddParameterWithValue(nameParam, users[i].Name);
                command.AddParameterWithValue(driverParam, users[i].IsDriver);
            }

            command.CommandText = sqlBuilder.ToString();

            // 3. Выполняем весь пакет за один сетевой "прыжок"
            int rowsAffected = command.ExecuteNonQuery();
            Console.WriteLine($"Успешно добавлено строк: {rowsAffected}");
        }
        catch (Exception ex)
        {
        }
        finally
        {
            _connection.Close();
        }
    }
    
    public void InsertUsersNpgsqlBatch(List<User> users) // только для PostgreSQL
    {
        // 1. Открываем соединение
        _connection.Open();

        try
        {
            // 2. Создаем объект батча
            using (var batch = new NpgsqlBatch((NpgsqlConnection)_connection))
            {
                foreach (var user in users)
                {
                    // 3. Создаем отдельную команду для каждой вставки
                    var batchCommand = new NpgsqlBatchCommand("""
                                                              INSERT INTO table_users (name, is_driver) 
                                                              VALUES ($1, $2);
                                                              """);

                    // Добавляем параметры (здесь можно использовать позиционные $1, $2 
                    // или именованные параметры, именовать уникально их уже не нужно!)
                    batchCommand.Parameters.AddWithValue(user.Name);
                    batchCommand.Parameters.AddWithValue(user.IsDriver);

                    // Добавляем команду в общий пакет
                    batch.BatchCommands.Add(batchCommand);
                }

                // 4. Выполняем весь пакет одним сетевым запросом
                int rowsAffected = batch.ExecuteNonQuery();
                Console.WriteLine($"[NpgsqlBatch] Успешно добавлено строк: {rowsAffected}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при выполнении NpgsqlBatch: {ex.Message}");
        }
        finally
        {
            _connection.Close();
        }
    }

    public void TransactionsDemoRawSql()
    {
        connection.Open();
        using var cmd = connection.CreateCommand();
        
        cmd.CommandText = """
                          BEGIN;
                            WITH inserted_user AS (
                              INSERT INTO table_users (name, is_driver) 
                              VALUES (@name, @isDriver) 
                              RETURNING id
                            )
                            INSERT INTO table_products (item_name, user_id, price, quantity)
                            SELECT @item_name, id, @price, @quantity 
                            FROM inserted_user;
                          COMMIT;
                          """;
        
        cmd.AddParameterWithValue("@name", "Raw SQL Transaction");
        cmd.AddParameterWithValue("@isDriver", true);

        cmd.AddParameterWithValue("@item_name", "Juice");
        cmd.AddParameterWithValue("@price", 120m);
        cmd.AddParameterWithValue("@quantity", "1 bottle");

        try
        {
            cmd.ExecuteNonQuery();
            Console.WriteLine("Raw SQL Транзакция (BEGIN/COMMIT) выполнена на стороне сервера.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Raw SQL Ошибка: {ex.Message}");
            cmd.CommandText = "ROLLBACK;";
            cmd.ExecuteNonQuery();
        }
        finally
        {
            _connection.Close();
        }
    }

    public void TransactionDemo()
    {
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        try
        {
            // 1. Вставляем пользователя
            using var userCmd = connection.CreateCommand();
            
            userCmd.CommandText =
                "INSERT INTO table_users (name, is_driver) VALUES (@name, @isDriver) RETURNING id";
            
            userCmd.Transaction = transaction;
            
            userCmd.AddParameterWithValue("@name", "Connected User Transaction Test from C#");
            userCmd.AddParameterWithValue("@isDriver", true);
            
            int newUserId = (int)userCmd.ExecuteScalar();
            
            
            using var prodCmd = connection.CreateCommand();
            prodCmd.CommandText =
                "INSERT INTO table_products (item_name, user_id,price, quantity) VALUES (@item_name, @user_id, @price, @quantity)";
            prodCmd.Transaction = transaction;
            
            prodCmd.AddParameterWithValue("@item_name", "Test C# Juiсe");
            prodCmd.AddParameterWithValue("@price", 100m);
            prodCmd.AddParameterWithValue("@quantity", "1 pcs");
            prodCmd.AddParameterWithValue("@user_id", newUserId);
            
            prodCmd.ExecuteNonQuery();
            
            transaction.Commit();
            Console.WriteLine("C# transaction Все данные записаны в БД");
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            Console.WriteLine($"C# transaction Ошибка: {ex.Message}. Откатываем.");
        }
        finally
        {
            connection.Close();
        }
    }
    
}