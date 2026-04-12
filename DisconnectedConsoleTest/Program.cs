using ADO.NET.DAL;
using ADO.NET.DAL.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DisconnectedConsoleTest;

class Program
{
    static void Main(string[] args)
    {
        var context =
            new DisconnectedNpgsqlContext(
                "Host=localhost:5437;Username=postgres;Password=1234;Database=bbq_db;Search path=test_schema");

        //var context = new DisconnectedNpgsqlContext();

        //context.DemonstrationOfRelations();
        
        /// // 1. Создаем фабрику логов, которая пишет в консоль
        //var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

        // 2. Указываем Npgsql использовать эту фабрику (делать один раз при старте)
       // NpgsqlLoggingConfiguration.InitializeLogging(loggerFactory);

        // var users = context.GetAllUsers();
        // foreach (var user in users) Console.WriteLine(user);

        //var testUser = new User() { Name = "Test user", IsDriver = false };
        //context.DemonstrateRowVersioning();

        //context.InsertUserWithAutoCommand(testUser);
        
        //context.InsertUserWithManualCommand(testUser);

        context.DemonstrateTransactionWithAdapter();
    }
}