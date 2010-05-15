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
			new H2Command("create table list (item integer primary key, description varchar(256))", connection).ExecuteNonQuery();
			new H2Command("insert into list values (1, 'First Item')", connection).ExecuteNonQuery();
			new H2Command("insert into list values (2, 'Second Item')", connection).ExecuteNonQuery();
			
			var table = new DataTable();
			var adapter = new H2DataAdapter("select item, description from list", connection);
			adapter.Fill(table);
			
			
			//var transaction = connection.BeginTransaction();
			
		}
	}
}

