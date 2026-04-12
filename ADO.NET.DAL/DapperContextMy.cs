using System.Data.Common;
using ADO.NET.DAL.Models;
using Dapper;

namespace ADO.NET.DAL;

public class DapperContextMy(DbConnection connection)
{
    private readonly DbConnection _connection = connection;
    
    
    // public IEnumerable<User> GetAllUsers()
    // {
    //     var result = new List<User>();
    //     _connection.Open();
    //
    //     const string sql = """
    //                         SELECT  id, name, is_driver
    //                         FROM table_users;
    //                        """;
    //
    //     var command = _connection.CreateCommand();
    //
    //     command.CommandText = sql;
    //
    //     var reader = command.ExecuteReader();
    //
    //     if (reader.HasRows)
    //     {
    //         while (reader.Read())
    //         {
    //             var id = reader.GetInt32("id");
    //             var isDriver = reader.GetBoolean("is_driver");
    //             var name = reader.GetString("name");
    //
    //             result.Add(new User()
    //             {
    //                 Id = id,
    //                 Name = name,
    //                 IsDriver = isDriver,
    //             });
    //         }
    //     }
    //     _connection.Close();
    //     return result;
    // }

    public IEnumerable<User> GetAllUsers()
    {
        var sql = """
                    SELECT id as Id, name as Name, is_driver as IsDriver 
                    FROM table_users;
                  """;
        return _connection.Query<User>(sql);
    }

    public IEnumerable<Product> GetAllProducts()
    {
        var sql = $"""
                  SELECT table_products.id as {nameof(Product.Id)},
                  item_name as {nameof(Product.Name)},
                  quantity as {nameof(Product.Quantity)},
                  price as {nameof(Product.Price)},
                  is_purchased as {nameof(Product.IsPurchased)},
                  table_users.name as  {nameof(Product.UserName)}
                  
                  FROM table_products
                      
                  LEFT JOIN table_users ON table_users.id = table_products.user_id;
                  """;
        return _connection.Query<Product>(sql);
    }

    public User? GetUserById(int userId)
    {
        var sql = $"""
                    SELECT id as {nameof(User.Id)}, name as {nameof(User.Name)}, is_driver as {nameof(User.IsDriver)} 
                    FROM table_users
                    WHERE id = @UserId;
                  """;
        return _connection.QuerySingle<User>(sql, new { UserId = userId });
    }

    public void InsertUser(User user)
    {
        var query = $"""
                     INSERT  INTO table_users
                     (name, is_driver)
                     VALUES (@{nameof(User.Name)}, @{nameof(User.IsDriver)});
                     """;
        _connection.Execute(query, user);
        
        // _connection.Execute(query, new
        // {
        //     Name = user.Name,
        //     IsDriver = user.IsDriver
        // });
    }

    public int InsertUsers(IEnumerable<User> users)
    {
        var query = $"""
                     INSERT  INTO table_users
                     (name, is_driver)
                     VALUES (@{nameof(User.Name)}, @{nameof(User.IsDriver)});
                     """;
        return _connection.Execute(query, users);
    }

    public void InsertUserWithUpdateId(User user)
    {
        var query = $"""
                     INSERT  INTO table_users (name,  is_driver)
                     VALUES ({nameof(user.Name)}, {nameof(user.IsDriver)})
                     RETURNING id;
                     """;
        user.Id =  _connection.QuerySingle<int>(query);
    }
    public void InsertUsersWithUpdateId(List<User> users)
    {
        var query = $"""
                     INSERT  INTO table_users (name,  is_driver)
                     VALUES ({nameof(User.Name)}, {nameof(User.IsDriver)})
                     RETURNING id;
                     """;
        foreach (var user in users)
        {
            user.Id =  _connection.QuerySingle<int>(query);
        }
    }

    public void CopyWithAddingProduct(string oldName, string newName)
    {
        string sql = """
                     INSERT INTO table_products
                     (item_name, quantity, price, is_purchased, user_id)
                     SELECT @NewName, quantity, price, is_purchased, user_id
                     FROM table_products
                     WHERE item_name = @OldName
                     """;
        _connection.Execute(sql, new { OldName = oldName, NewName = newName });
    }
    
    public void BuyProduct(string productName, decimal price)
    {
        string sql = """
                     UPDATE  table_products 
                     SET price = @Price, is_purchased = TRUE
                     WHERE item_name = @ProductName;
                     """;
        _connection.Execute(sql, new { Price = price, ProductName = productName });
    }

    public void AddProductsTransaction(IEnumerable<Product> products)
    {
        _connection.Open();

        var transaction = _connection.BeginTransaction();
        try
        {
            var sql = $"""
                      INSERT INTO table_products
                      (item_name, quantity, price, is_purchased)
                      VALUES (@{nameof(Product.Name)},
                              @{nameof(Product.Quantity)},
                              @{nameof(Product.Price)}, 
                              @{nameof(Product.IsPurchased)});
                      """;
            _connection.Execute(sql, products,  transaction);
            transaction.Commit();
            Console.WriteLine("Вызван Commit");
        }
        catch
        {
            transaction.Rollback();
            Console.WriteLine("Вызван Rollback");
            transaction.Dispose();

        }
        finally
        {
            _connection.Close();
        }
    }
    
    public IEnumerable<string> GetProductsName() =>
        _connection.Query<string>("SELECT item_name FROM table_products");

    public void MultipleDemo(int id)
    {
        string sql = $"""
                      SELECT id as {nameof(User.Id)}, 
                      name as {nameof(User.Name)}, 
                      is_driver as  {nameof(User.IsDriver)}
                      FROM table_users
                      WHERE id = @Id;

                      SELECT table_products.id as {nameof(Product.Id)},
                             item_name as {nameof(Product.Name)},
                             quantity as {nameof(Product.Quantity)},
                             price as {nameof(Product.Price)},
                             is_purchased as {nameof(Product.IsPurchased)},
                             name as  {nameof(Product.UserName)}
                      FROM table_products
                      LEFT JOIN table_users  ON table_products.user_id = table_users.id
                      WHERE table_products.id = @Id;

                      SELECT COUNT(*) FROM table_users;
                             
                      """;
        using var multi = _connection.QueryMultiple(sql, new { Id = id });
        
        var user = multi.ReadSingle<User>();
        var product = multi.ReadSingle<Product>();
        var usersCount = multi.ReadSingle<int>();

        Console.WriteLine($"User: {user} \nProduct: {product} \n Количество пользователей: {usersCount}");
       
    }

    public IEnumerable<User> GetUsersByIds(int[] ids)
    {
        // [1,2,3...]
        // ANY(1,2,3...)
        var sql = "SELECT * FROM table_users WHERE id = ANY(@Ids)";
        return _connection.Query<User>(sql, new { Ids = ids });
    }

    public IEnumerable<Product> GetProducts()
    {
        var sql = $"""
                   SELECT p.id as  {nameof(Product.Id)},
                          p.item_name as  {nameof(Product.Name)},
                          p.quantity as  {nameof(Product.Quantity)},
                          p.price as  {nameof(Product.Price)},
                          p.is_purchased as {nameof(Product.IsPurchased)},
                          u.id as {nameof(User.Id)},
                          u.name as {nameof(User.Name)},
                          u.is_driver as {nameof(User.IsDriver)}
                   FROM table_products as p
                   LEFT JOIN table_users as u ON u.id = p.user_id
                   """;
        return _connection.Query<Product, User, Product>(
            sql,
            (product, user) =>
            {
                product.User = user;
                return product;
            }, 
            splitOn: "id"
        );
    }
    
    
    
}