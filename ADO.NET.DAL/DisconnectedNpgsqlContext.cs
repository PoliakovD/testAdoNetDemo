using System.Data;
using ADO.NET.DAL.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ADO.NET.DAL;

public class DisconnectedNpgsqlContext
{
    private readonly string _connectionString;
    private readonly DataSet _dataSet = new("BBQ_DataSet");

    public DisconnectedNpgsqlContext(string connectionString)
    {
        _connectionString = connectionString;
        using var connection = new NpgsqlConnection(_connectionString);

        // 1. Заполняем таблицу пользователей
        var userAdapter = new NpgsqlDataAdapter(
            "SELECT id, name, is_driver FROM table_users", connection);
        userAdapter.Fill(_dataSet, "table_users");

        // 2. Заполняем таблицу продуктов
        var productAdapter =
            new NpgsqlDataAdapter("SELECT id, item_name, quantity, price, is_purchased, user_id FROM table_products",
                connection);
        productAdapter.Fill(_dataSet, "table_products");

        // Устанавливаем первичные ключи для работы поиска в памяти
        _dataSet.Tables["table_users"]!.PrimaryKey = new[] { _dataSet.Tables["table_users"]!.Columns["id"]! };
        _dataSet.Tables["table_products"]!.PrimaryKey = new[] { _dataSet.Tables["table_products"]!.Columns["id"]! };

        // Создаём отношение между таблицами
        var relation = new DataRelation(
            "UserProducts",
            _dataSet.Tables["table_users"].Columns["id"],
            _dataSet.Tables["table_products"].Columns["user_id"]
        );
        _dataSet.Relations.Add(relation);
        
        
        // var userBuilder = new NpgsqlCommandBuilder(userAdapter);
        // var productBuilder = new NpgsqlCommandBuilder(productAdapter);
        
        // var upd = userBuilder.GetUpdateCommand().CommandText;
        // var insert = userBuilder.GetInsertCommand().CommandText;
        // var delete = userBuilder.GetDeleteCommand().CommandText;
        // Console.WriteLine("user upd: " + upd);
        // Console.WriteLine();
        // Console.WriteLine("user insert: " + insert);
        // Console.WriteLine();
        // Console.WriteLine("user delete: " + delete);
        // Console.WriteLine();
        //
        // var pupd = productBuilder.GetUpdateCommand().CommandText;
        // var pinsert = productBuilder.GetInsertCommand().CommandText;
        // var pdelete = productBuilder.GetDeleteCommand().CommandText;
        // Console.WriteLine("product upd: " + pupd);
        // Console.WriteLine();
        // Console.WriteLine("product insert: " + pinsert);
        // Console.WriteLine();
        // Console.WriteLine("product delete: " + pdelete);
        // Console.WriteLine();
    }

    public DisconnectedNpgsqlContext()
    {
        // Инициализируем DataSet
        _dataSet = new DataSet();

        // Создаём таблицу Users
        DataTable usersTable = new DataTable("table_users");
        usersTable.Columns.Add("id", typeof(int));
        usersTable.Columns.Add("name", typeof(string));
        usersTable.Columns.Add("is_driver", typeof(bool));

        // Добавляем тестовые данные в таблицу Users
        usersTable.Rows.Add(1, "Алиса", true);
        usersTable.Rows.Add(2, "Боб", false);
        usersTable.Rows.Add(3, "Никита", true);

        // Создаём таблицу Products
        DataTable productsTable = new DataTable("table_products");
        productsTable.Columns.Add("id", typeof(int));
        productsTable.Columns.Add("item_name", typeof(string));
        productsTable.Columns.Add("quantity", typeof(string));
        productsTable.Columns.Add("price", typeof(decimal));
        productsTable.Columns.Add("is_purchased", typeof(bool));

        // var userIdColumn = new DataColumn("user_id", typeof(int))
        // {
        //     AllowDBNull = true // ✅ Вот так разрешается NULL
        // };
        // productsTable.Columns.Add(userIdColumn);

        productsTable.Columns.Add("user_id", typeof(int)).AllowDBNull = true;

        // Добавляем тестовые данные в таблицу Products
        productsTable.Rows.Add(1, "Мангал", "1", 299.99m, true, 1);
        productsTable.Rows.Add(2, "Уголь", "5кг", 25.50m, false, 1);
        productsTable.Rows.Add(3, "Соус томатный", "2 пачки", 12.99m, true, 2);
        productsTable.Rows.Add(4, "помидоры", "3 шт", 18.00m, true, null);

        // Добавляем таблицы в DataSet
        _dataSet.Tables.Add(usersTable);
        _dataSet.Tables.Add(productsTable);

        // Устанавливаем PrimaryKey для связи
        _dataSet.Tables["table_users"].PrimaryKey = [_dataSet.Tables["table_users"].Columns["id"]];
        _dataSet.Tables["table_products"].PrimaryKey = [_dataSet.Tables["table_products"].Columns["id"]];

        // Создаём отношение между таблицами
        var relation = new DataRelation(
            "UserProducts",
            _dataSet.Tables["table_users"].Columns["id"],
            _dataSet.Tables["table_products"].Columns["user_id"]
        );
        _dataSet.Relations.Add(relation);

        Console.WriteLine("DataSet инициализирован с тестовыми данными (режим оффлайн).");
    }

    public void DemonstrationOfRelations()
    {
        var usersTable = _dataSet.Tables["table_users"];
        foreach (DataRow userRow in usersTable.Rows)
        {
            foreach (DataRow productRow in userRow.GetChildRows("UserProducts"))
            {
                Console.WriteLine($"Пользователь: {userRow["name"]} Товар: {productRow["item_name"]}");
            }
        }
    }

    public IEnumerable<User> GetAllUsers()
    {
        var usersTable = _dataSet.Tables["table_users"]!;

        return usersTable.AsEnumerable().Select(row => new User
        {
            Id = row.Field<int>("id"),
            Name = row.Field<string>("name") ?? "",
            IsDriver = row.Field<bool>("is_driver")
        });
    }

    public void AddLogging()
    {
        /// // 1. Создаем фабрику логов, которая пишет в консоль
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

        // 2. Указываем Npgsql использовать эту фабрику (делать один раз при старте)
        NpgsqlLoggingConfiguration.InitializeLogging(loggerFactory);
    }
    
    public void DemonstrateRowVersioning()
    {
        Console.WriteLine("=== ДЕМОНСТРАЦИЯ ВЕРСИОННОСТИ DATAROW ===\n");

        // 1. Подготовка таблицы (имитируем данные из БД)
        // DataTable table = new DataTable("Users");
        // table.Columns.Add("id", typeof(int));
        // table.Columns.Add("name", typeof(string));
        // table.PrimaryKey = [table.Columns["id"]!];

        var table = _dataSet.Tables["table_users"];

        // Добавляем строку и фиксируем её (AcceptChanges), как будто она пришла из БД
        
        DataRow row = table.Rows[0];
        Console.WriteLine($"Исходное состояние: {row["name"]} (Статус: {row.RowState})");

        // 2. Начало редактирования
        Console.WriteLine("\n--- Меняем имя на 'Danya_Developer' ---");
        row["name"] = "Danya_Developer";

        // 3. Вывод разных версий
        // Мы можем обратиться к данным, указывая конкретную версию
        if (row.HasVersion(DataRowVersion.Original))
        {
            Console.WriteLine($"Версия Original (как в БД): {row["name", DataRowVersion.Original]}");
        }

        if (row.HasVersion(DataRowVersion.Current))
        {
            Console.WriteLine($"Версия Current (правка юзера): {row["name", DataRowVersion.Current]}");
        }

        Console.WriteLine($"Текущий статус строки: {row.RowState}");

        // 4. Демонстрация отката (RejectChanges)
        Console.WriteLine("\n--- Выполняем RejectChanges() (Откат) ---");
        row.RejectChanges();

        Console.WriteLine($"После отката (Current стал равен Original): {row["name"]}");
        Console.WriteLine($"Статус строки: {row.RowState}");

        // 5. Демонстрация принятия (AcceptChanges)
        Console.WriteLine("\n--- Снова меняем и фиксируем (AcceptChanges) ---");
        row["name"] = "Senior_Danya";
        row.AcceptChanges(); // Теперь Original затирается значением Current

        Console.WriteLine($"После AcceptChanges:");
        Console.WriteLine($"Current: {row["name", DataRowVersion.Current]}");
        Console.WriteLine($"Original (теперь равен Current): {row["name", DataRowVersion.Original]}");
        Console.WriteLine($"Статус строки: {row.RowState}");
    }
    
    public void InsertUserWithAutoCommand(User user)
    {
        using var connection = new NpgsqlConnection(_connectionString);

        // 1. Создаем адаптер
        var adapter = new NpgsqlDataAdapter("SELECT * FROM table_users", connection);
        var builder = new  NpgsqlCommandBuilder(adapter);
        
        DataRow newRow = _dataSet.Tables["table_users"]!.NewRow();
        newRow["id"] = user.Id;
        newRow["name"] = user.Name;
        newRow["is_driver"] = user.IsDriver;
        _dataSet.Tables["table_users"].Rows.Add(newRow);
        
        Console.WriteLine($"До вставки: ID = {newRow["id"]}");
        
        adapter.Update(_dataSet, "table_users");
        
        Console.WriteLine($"После вставки: ID из базы = {newRow["id"]}");
        
        // newRow.RowState = DataRowState.Added;
        // newRow.RowState = DataRowState.Deleted;
        // newRow.RowState = DataRowState.Detached;
        // newRow.RowState = DataRowState.Modified;
        // newRow.RowState = DataRowState.Unchanged;
        
    }
    
    public void InsertUserWithManualCommand(User user)
    {
        using var connection = new NpgsqlConnection(_connectionString);

        // 1. Создаем адаптер
        var adapter = new NpgsqlDataAdapter("SELECT * FROM table_users", connection);

        // 2. ВРУЧНУЮ создаем команду вставки
        // Используем RETURNING id, чтобы база сразу вернула созданный ключ
        var insertCmd = new NpgsqlCommand(
            "INSERT INTO table_users (name, is_driver) VALUES (@name, @isDriver) RETURNING id",
            connection);

        // Добавляем параметры
        insertCmd.Parameters.Add("@name", NpgsqlTypes.NpgsqlDbType.Varchar, 100, "name");
        insertCmd.Parameters.Add("@isDriver", NpgsqlTypes.NpgsqlDbType.Boolean, 0, "is_driver");

        // КРИТИЧЕСКИЙ МОМЕНТ: Говорим адаптеру, что результат команды (ID) 
        // нужно записать в первую колонку измененной строки
        insertCmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;

        //.FirstReturnedRecord; Как работает: Адаптер ожидает, что SQL-запрос вернет обычную строку данных (как SELECT).
        //Он берет первую строку первого набора результатов и сопоставляет её колонки с колонками в вашей DataTable.

        // .OutputParameters Используется, когда данные возвращаются не через SELECT/RETURNING, а через специальные переменные — выходные параметры.
        // Как работает: Адаптер игнорирует результат самого запроса, но проверяет коллекцию Parameters у команды.
        // Если у параметра стоит Direction = ParameterDirection.Output (или InputOutput), адаптер возьмет его новое значение и запишет в привязанную колонку.
        //    Где применяется: Чаще всего при работе с Хранимыми процедурами (Stored Procedures).

        //.Both
        // Гибридный режим: «и то, и другое».

        // Как работает: Адаптер сначала считывает данные из первой вернувшейся строки (FirstReturnedRecord),
        // а затем обновляет данные из выходных параметров (OutputParameters).

        //    Где применяется: В сложных корпоративных системах или легаси-коде, где хранимая процедура может и возвращать
        // таблицу, и одновременно заполнять выходные параметры (например, статус операции в параметр, а ID — через SELECT).

        // UpdateRowSource.None
        //Режим «молчуна».
        //Как работает: Адаптер просто выполняет команду в базе и ничего не пытается прочитать в ответ.
        //это результат работы сопоставления имен (Name Mapping) между базой данных и вашей DataTable


        adapter.InsertCommand = insertCmd;

        // 3. Работаем в Disconnected Mode
        var dt = _dataSet.Tables["table_users"];
        

        // Добавляем новую строку в память (ID пока пустой или 0)
        DataRow newRow = dt.NewRow();
        newRow["id"] = 999999;
        newRow["name"] = user.Name;
        newRow["is_driver"] = user.IsDriver;
        dt.Rows.Add(newRow);


        Console.WriteLine($"До вставки: ID = {newRow["id"]}");

        // 4. Синхронизируем
        adapter.Update(dt);

        // Благодаря UpdatedRowSource.FirstReturnedRecord, ID обновился АВТОМАТИЧЕСКИ
        Console.WriteLine($"После вставки: ID из базы = {newRow["id"]}");
    }
    
     public void DemonstrateTransactionWithAdapter()
    {
        // 2. Что происходит внутри метода .Update()?
        //     Когда вы вызываете Update(), адаптер начинает перебирать строки в DataTable.
        //     Если транзакция назначена, каждая команда (INSERT/UPDATE/DELETE) выполняется в рамках этого единого канала.
        //     Если в середине процесса (например, на 50-й строке из 100) база данных вернет ошибку, адаптер выбросит исключение.
        //     Важно: Сама база данных откатит только ту команду, которая упала. Чтобы откатить предыдущие 49 успешно вставленных строк,
        //      вы обязательно должны вызвать transaction.Rollback() в блоке catch.
        // 3. RowState и Транзакции (Тонкий момент)
        //      Метод Update() автоматически вызывает AcceptChanges() для каждой строки после успешного выполнения команды.
        //     Проблема: Если вы делаете Update(), строки помечаются как Unchanged. Но если после этого транзакция сорвется и вы сделаете Rollback(),
        //      данные в базе откатятся, а в вашей DataTable они останутся помеченными как сохраненные!
        //     Решение: Перед вызовом Update установите adapter.AcceptChangesDuringUpdate = false;. Тогда вы сможете вызвать dataTable.AcceptChanges() вручную только после успешного transaction.Commit().

        using var connection = new NpgsqlConnection(_connectionString);
        connection.Open();

        // Начинаем транзакцию
        using var transaction = connection.BeginTransaction();

        try
        {
            // 4. Пытаемся сохранить оба изменения в одной транзакции
            Console.WriteLine("Начинаем транзакцию...");
            
            // 1. Адаптер для пользователей
            var userAdapter = new NpgsqlDataAdapter(
                "SELECT id, name, is_driver FROM table_users", connection);

            userAdapter.AcceptChangesDuringUpdate = false; // см выше
            
            var insertCmd = new NpgsqlCommand(
                "INSERT INTO table_users (name, is_driver) VALUES (@name, @isDriver) RETURNING id",
                connection);
            insertCmd.Parameters.Add("@name", NpgsqlTypes.NpgsqlDbType.Text, 0, "name");
            insertCmd.Parameters.Add("@isDriver", NpgsqlTypes.NpgsqlDbType.Boolean, 0, "is_driver");

           

            // Создаём команды вручную или через CommandBuilder
            userAdapter.InsertCommand = insertCmd;
          

           // Указываем, что нужно обновить строку данными из первого вернувшегося рекорда
            userAdapter.InsertCommand.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;

          
         
            
            // UPDATE
            userAdapter.UpdateCommand = new NpgsqlCommand(
                "UPDATE table_users SET name = @name, is_driver = @is_driver WHERE id = @id", connection);
            userAdapter.UpdateCommand.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Integer, 0, "id");
            userAdapter.UpdateCommand.Parameters["@id"].SourceVersion = DataRowVersion.Original;
            userAdapter.UpdateCommand.Parameters.Add("@name", NpgsqlTypes.NpgsqlDbType.Text, 0, "name");
            userAdapter.UpdateCommand.Parameters.Add("@is_driver", NpgsqlTypes.NpgsqlDbType.Boolean, 0, "is_driver");

            // DELETE
            userAdapter.DeleteCommand = new NpgsqlCommand(
                "DELETE FROM table_users WHERE id = @id", connection);
            userAdapter.DeleteCommand.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Integer, 0, "id");
            userAdapter.DeleteCommand.Parameters["@id"].SourceVersion = DataRowVersion.Original;
            
            // Привязываем транзакцию к адаптеру через команды
            userAdapter.SelectCommand.Transaction = transaction;
            // Устанавливаем транзакцию для всех команд
            userAdapter.InsertCommand.Transaction = transaction;
            userAdapter.UpdateCommand.Transaction = transaction;
            userAdapter.DeleteCommand.Transaction = transaction;
            
            
            // 2. Адаптер для продуктов
            var productAdapter = new NpgsqlDataAdapter(
                "SELECT id, item_name, quantity, price, is_purchased, user_id FROM table_products", connection);

            productAdapter.AcceptChangesDuringUpdate = false; // см выше

            productAdapter.SelectCommand.Transaction = transaction;

            var productBuilder = new NpgsqlCommandBuilder(productAdapter);

            productAdapter.InsertCommand = productBuilder.GetInsertCommand();
            productAdapter.UpdateCommand = productBuilder.GetUpdateCommand();
            productAdapter.DeleteCommand = productBuilder.GetDeleteCommand();

            productAdapter.InsertCommand.Transaction = transaction;
            productAdapter.UpdateCommand.Transaction = transaction;
            productAdapter.DeleteCommand.Transaction = transaction;

            // 3. Вносим изменения в DataSet (в памяти)
            var usersTable = _dataSet.Tables["table_users"];
            var productsTable = _dataSet.Tables["table_products"];

            // Добавляем нового пользователя
            var newUserRow = usersTable.NewRow();
            
            
            
            newUserRow["id"] = 0;
            newUserRow["name"] = "Transaction User";
            newUserRow["is_driver"] = false;
            usersTable.Rows.Add(newUserRow);
            
            // начинаем что бы получить обратно новый id
            userAdapter.Update(usersTable);
            Console.WriteLine($"✅ Пользователь с ID={newUserRow["id"]} добавлен.");
            
            // Добавляем товар для этого пользователя (ссылка на новый ID)
            var newProductRow = productsTable.NewRow();
            newProductRow["id"] = 0;
            newProductRow["item_name"] = "Grill (transaction)";
            newProductRow["quantity"] = "1";
            // newProductRow["price"] = -399.99m;
            newProductRow["price"] = 399.99m;
            newProductRow["is_purchased"] = true;
            newProductRow["user_id"] = newUserRow["id"]; // Связь
            productsTable.Rows.Add(newProductRow);
           
            productAdapter.Update(productsTable);

            // Если всё прошло успешно — фиксируем изменения
            transaction.Commit();

            productsTable.AcceptChanges(); // подтверждаем изменения
            usersTable.AcceptChanges(); // подтверждаем изменения

            Console.WriteLine("✅ Транзакция завершена успешно. Все изменения сохранены.");
        }
        catch (Exception ex)
        {
            // При любой ошибке — откатываем всё
            transaction.Rollback();
            Console.WriteLine($"❌ Ошибка в транзакции: {ex.Message}");
            Console.WriteLine("🔄 Все изменения отменены (Rollback).");
            
        }
    }
}