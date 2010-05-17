using System;
using System.Data;
using System.Data.H2;
using System.Data.Common;
using System.IO;
using System.Diagnostics;
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
			
			var table = new DataTable("test") {
				CaseSensitive = false
			};
            var adapter = new H2DataAdapter("select item, description, value from list order by item", connection);
			new H2CommandBuilder(adapter);
			adapter.Fill(table);
			
			table.Rows[1][1] = "First item modified";
			table.Rows[1][2] = 12;
            table.Rows.Add(new object[] { 3, "Third", 15 });
			adapter.Update(table);
			
            var table2 = new DataTable("test") {
                CaseSensitive = false
            };
            adapter.Fill(table2);

            var x1 = table.ToXml();
            var x2 = table2.ToXml();
            Debug.Assert(x1.Equals(x2));

            var count = new H2Command("select count(*) from list", connection).ExecuteScalar();
            Debug.Assert(((long)count).Equals(3));
            
            var one = new H2Command("select 1 from dual", connection).ExecuteScalar();
            Debug.Assert(((int)one).Equals(1));
            
            var a = new H2Command("select 'a' from dual", connection).ExecuteScalar();
            Debug.Assert(((string)a).Equals("a"));
            
            //var x1 = table.ToXml();
		}
		
	}
    
    public static class DataTableExtensions
    {
        public static String ToXml(this DataTable table)
        {
            var o = new MemoryStream();
            table.WriteXml(o);
            var t = new StringWriter();
			o.Close();
			o = new MemoryStream(o.GetBuffer());
			var reader = new StreamReader(o);
			return reader.ReadToEnd();
        }
    }
}

