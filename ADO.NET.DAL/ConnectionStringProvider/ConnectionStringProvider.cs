using System.Reflection;
using Microsoft.Extensions.Configuration;

namespace ADO.NET.DAL.ConnectionStringProvider;

public static class ConnectionStringProvider
{
    // Nuget Packages
    
    // Microsoft.Extensions.Configuration
    
    // Microsoft.Extensions.Configuration.Xml
    
    // Microsoft.Extensions.Configuration.Json
    
    // Microsoft.Extensions.Configuration.UserSecrets
    
    // Microsoft.Extensions.Configuration.EnvironmentVariables
    
    // DotNetEnv 

    public static string GetConnectionString()
    {
        var builder = new ConfigurationBuilder()
            .AddJsonFile("ConnectionStringProvider\\appsettings.json", optional: true, reloadOnChange: true);
            //.AddEnvironmentVariables();
        
        IConfiguration config = builder.Build();

        return config.GetConnectionString("Default");

    }

    public static string GetConnectionStringFromXml()
    {
        // Создаем строитель конфигурации
        var builder = new ConfigurationBuilder()
            .AddXmlFile("ConnectionStringProvider\\config.xml");
        
        var config = builder.Build();

        return config.GetConnectionString("Default")!;
    }
    public static string GetConnectionStringFromJson()
    {
        // Создаем строитель конфигурации
        var builder = new ConfigurationBuilder()
            .AddJsonFile("ConnectionStringProvider\\appsettings.json");
        
        var config = builder.Build();

        return config.GetConnectionString("Default")!;
    }
    
    public static string GetConnectionStringFromUserSecrets()
    {
        // Создаем строитель конфигурации
        var builder = new ConfigurationBuilder()
            .AddUserSecrets(Assembly.GetExecutingAssembly());
            // .AddUserSecrets("2750c873-9064-4a28-8a73-0d11451a97e2");
        
        var config = builder.Build();

        return config.GetConnectionString("Default")!;
    }
    
    public static string GetConnectionStringFromEnv()
    {
        
        //DotNetEnv.Env.Load();

        foreach (var line in File.ReadAllLines(".env"))
        {
            var  parts = line.Split('=',2);
            if (parts.Length != 2) continue;
            Environment.SetEnvironmentVariable(parts[0], parts[1]);
        }
        
        
        // Создаем строитель конфигурации
        var builder = new ConfigurationBuilder()
            .AddEnvironmentVariables();
        
        var config = builder.Build();

        return config.GetConnectionString("Default")!;
    }
    
}