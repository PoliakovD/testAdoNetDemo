using System;
using ADO.NET.DAL;
using ADO.NET.DAL.Models;

namespace ADONetTest.Connected;

class Program
{
    static void Main(string[] args)
    {
        try
        {
            //docker compose -f compose.yaml -p bbqdatabase up -d
            
            
            //Флаг -p (сокращение от --project-name) в команде docker compose используется для задания имени проекта.
        
            //-- Флаг -d запускает контейнеры в фоновом режиме, освобождая терминал;
            // фвывыв
            var connection = DbConnectionFactory.GetPostgreSqlConnection();
            //var connection = DbConnectionFactory.GetSqLiteConnection();
            //var connection =DbConnectionFactory.GetMySqlConnection();
            
            var context = new ConnectedContext(connection);

            // var productList = context.GetAllProducts();
            // foreach (var product in productList)
            // {
            //     Console.WriteLine($"{product.Id}-{product.Name} - {product.Price} - {product.Quantity} - {product.UserName}");
            // }
            //
            // var newUser = new User()
            // {
            //     Name = "John Doe",
            //     IsDriver = true,
            // };
            //
            // context.InsertNewUser(newUser);
            
            
            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            
            
            // var users = context.GetAllUsers();
            //
            //  Console.WriteLine("До SQL Инъекции");
            //  foreach (var user in users)
            //      {
            //          Console.WriteLine($"{user.Id}-{user.Name} - [{(user.IsDriver?"Водитель":"Пешеход")}]");
            //      }
            //
            //   var newUser = new User()
            //  {
            //      Name = "1',false); DROP TABLE table_users; --",
            //      IsDriver = true,
            //  };
            //
            //  context.InsertNewUser(newUser);
            //
            //  Console.WriteLine("После SQL Инъекции");
            //
            //  users = context.GetAllUsers();
            //
            //  Console.WriteLine("До SQL Инъекции");
            //  foreach (var user in users)
            //  {
            //      Console.WriteLine($"{user.Id}-{user.Name} - [{(user.IsDriver?"Водитель":"Пешеход")}]");
            //  }
            //
            
            
            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            
            
            // var newUser = new User()
            // {
            //     Name = "1',false); DROP TABLE table_users; --",
            //        IsDriver = true,
            // };
            //
            // context.InsertNewUserWithParameters(newUser);
            //
            // var users = context.GetAllUsers();
            // foreach (var user in users)
            //     {
            //         Console.WriteLine($"{user.Id}-{user.Name} - [{(user.IsDriver?"Водитель":"Пешеход")}]");
            //     }
            //
            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            var avg = context.GetAvgPrice();
            Console.WriteLine("Avg Price: " + avg);
            
            Console.WriteLine("Старая цена для шеи:"+ context.UpdatePrice(1, 3500m));
            
             avg = context.GetAvgPrice();
            Console.WriteLine("Avg Price: " + avg);
            

            context.TransactionsDemoRawSql();
            //context.TransactionDemo();
            
            
            Console.ReadLine();
        }
        catch (Exception e)
        {
            Console.WriteLine("Ошибка: " + e.Message);
        }
        

    }
}