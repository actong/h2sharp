using System;
using System.Data;
using System.Data.H2;
namespace H2Examples
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Hello World!");
			
			var connection = new H2Connection("jdbc:h2:mem:test");
			connection.Open();
			new H2Command("create table list (item integer primary key, description varchar(256), value integer)", connection).ExecuteNonQuery();
			new H2Command("insert into list values (1, 'First Item', 10)", connection).ExecuteNonQuery();
			new H2Command("insert into list values (2, 'Second Item', 11)", connection).ExecuteNonQuery();
			
			var table = new DataTable() {
				CaseSensitive = false
			};
			var adapter = new H2DataAdapter("select item, value from list", connection);
			var commandBuilder = new H2CommandBuilder(adapter);
			adapter.Fill(table);
			
			//table.Rows[1][1] = "First item modified";
			table.Rows[1][1] = 12;
			adapter.Update(table);
			//var transaction = connection.BeginTransaction();
			
		}
		
	}
}

