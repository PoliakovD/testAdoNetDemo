using ADO.NET.DAL;
using ADO.NET.DAL.Models;

namespace DapperTest;

class Program
{
    static void Main(string[] args)
    {
        var connection = DbConnectionFactory.GetPostgreSqlConnection();
        
        var dapperContext = new DapperContextMy(connection);
        
        // тест
        //
        // var allUsers = dapperContext.GetAllUsers();
        // foreach (var user in allUsers)
        // {
        //     Console.WriteLine($"{user.Id} -- {user.Name} -- {user.IsDriver}");
        // }
        
        // var allProducts = dapperContext.GetAllProducts();
        // foreach (var product in allProducts)
        // {
        //     Console.WriteLine($"{product.Id} -- {product.Name} -- {product.Price} -- {product.Quantity} -- {product.UserName}");
        // }
        //
        
        
        // var thirdUser = dapperContext.GetUserById(3);
        // var thirdUser2 = dapperContext.GetUserById(3);
        //
        //
        // Console.WriteLine(thirdUser);
        // Console.WriteLine(thirdUser2);
        //
        // Console.WriteLine(thirdUser==thirdUser2);
        
        // var allProducts = dapperContext.GetAllProducts();
        // foreach (var product in allProducts)
        // {
        //     Console.WriteLine(product);
        // }
        //

        var newUser1 = new User()
        {
            Name = "Dapper User 1",
            IsDriver = false
        };
        var newUser2 = new User()
        {
            Name = "Dapper User 2",
            IsDriver = false
        };
        var newUser3 = new User()
        {
            Name = "Dapper User 3",
            IsDriver = false
        };

        List<User> listUser = [newUser1,newUser2,newUser3];

        
        var rows = dapperContext.InsertUsers(listUser);

        Console.WriteLine("Inserted Successfully" + rows);

    }
}