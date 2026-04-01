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

        if (reader.HasRows)
        {
            while (reader.Read())
            {
                var id = reader.GetInt32("id");
                var itemName = reader.GetString("item_name");
                var quantity = reader.GetString("quantity");
                var price = reader.GetDecimal("price");
                var isPurchased = reader.GetBoolean("is_purchased");
                
                
                //var userName = reader.GetString("name");
                var userName = reader.IsDBNull("name") ? "Еще никто не взялся!" : reader.GetString("name");
                
                result.Add(new Product()
                {
                    Id = id,
                    IsPurchased = isPurchased,
                    UserName = userName,
                    Name = itemName,
                    Quantity = quantity,
                    Price = price,
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
        command.AddParameterWithValue("@p_old_price", null,ParameterDirection.Output,DbType.Decimal);
        
        

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

}