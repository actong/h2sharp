using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.H2;
using System.Text.RegularExpressions;
using System.Data;

namespace System.Data.H2
{
    /// <summary>
	/// This command builder is still buggy, please only use it to debug it :-)
	/// </summary>
	public class H2CommandBuilder
    {
        H2Connection connection;
        public H2CommandBuilder(H2DataAdapter adapter)
        {
            connection = adapter.SelectCommand.Connection;
            var select = adapter.SelectCommand.CommandText.ToLower();
            var rx = new Regex("select (.*) from (.*)(?: order by (?:.*))?");
            var mat = rx.Match(select);
            if (!mat.Success)
                throw new Exception("Select command not recognized : '" + select + "'");

            var tableName = mat.Groups[2].Value;
            {
                var rrx = new Regex("\"(.*)\"");
                var mmat = rrx.Match(tableName);
                if (mmat.Success)
                    tableName = mmat.Groups[1].Value;
            }

            var columnTypes = connection.GetColumnsDataType(tableName);

            IList<String> cols = mat.Groups[1].Value.Split(',');
            if (cols.Count == 1 && cols[0].Trim().Equals("*"))
                cols = columnTypes.Keys.ToList();

            var updateCommand = new H2Command(connection);
            var insertCommand = new H2Command(connection);
            var updateSets = new List<String>();
            var updateWheres = new List<String>();
            //var namesUp = new List<String>();
            //var valuesUp = new List<String>();
            var colasrx = new Regex("\"?(.*)\"? as \"?(.*)\"?");
            int nextParam = 0;
            var aliases = new Dictionary<String, String>();
            foreach (var col in cols)
            {
                var colasmat = colasrx.Match(col);
                String alias;
                String columnName;
                if (colasmat.Success)
                {
                    alias = colasmat.Groups[2].Value;
                    columnName = colasmat.Groups[1].Value;
                    aliases[columnName] = alias;
                }
                else
                {
                    columnName = alias = col;
                }

                var paramName = (nextParam++).ToString();

                updateSets.Add("\"" + columnName + "\" = ?");//:" + paramName);

                updateCommand.Parameters.Add(new H2Parameter(paramName, columnTypes[columnName])
                {
                    SourceColumn = alias,
                    DbType = columnTypes[columnName],
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Current
                });

            }
            var pksList = connection.ReadString("SELECT column_list FROM INFORMATION_SCHEMA.CONSTRAINTS where lower(table_name) = '" + tableName.ToLower() + "' and constraint_type = 'PRIMARY KEY'");
            var pks = pksList == null ? cols : pksList.Split().Select(s => s.Trim());
            foreach (var pk in pks)
            {
                var columnName = pk;
                var paramName = (nextParam++).ToString();
                updateWheres.Add("\"" + columnName + "\" = ?");//:" + paramName);

                String alias;
                if (!aliases.TryGetValue(columnName, out alias))
                    alias = columnName;
                updateCommand.Parameters.Add(new H2Parameter(paramName, columnTypes[columnName])
                {
                    SourceColumn = alias,
                    DbType = columnTypes[columnName],
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                });
            }
            var insertValues = new List<String>();
            nextParam = 0;
            foreach (var columnName in cols)
            {
                var paramName = (nextParam++).ToString();
                insertValues.Add("?");//":" + paramName);
                String alias;
                if (!aliases.TryGetValue(columnName, out alias))
                    alias = columnName;
                insertCommand.Parameters.Add(new H2Parameter(paramName, columnTypes[columnName])
                {
                    SourceColumn = alias,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                });
            }
            updateCommand.CommandText = "update " + tableName + " set " + updateSets.Commas() + " where " + updateWheres.Commas();
            adapter.UpdateCommand = updateCommand;
            insertCommand.CommandText = "insert into " + tableName + "(" + cols.Commas() + ") values (" + insertValues.Commas() + ")";
            adapter.InsertCommand = insertCommand;
        }
    }
	public static class ConnectionExtensions
    {
        public static Dictionary<String, DbType> GetColumnsDataType(this H2Connection connection, String tableName)
        {
            var columnNamesAndValues = connection.ReadMap<String>("select column_name, type_name from INFORMATION_SCHEMA.columns where lower(table_name) = '" + tableName.ToLower() + "'");
            var ret = new Dictionary<String, DbType>();
            foreach (var kv in columnNamesAndValues)
            {
                DbType type;
                switch (kv.Value)
                {
                    case "VARCHAR":
                    case "VARCHAR2":
                        type = DbType.StringFixedLength;
                        break;
                    case "INT":
                    case "INTEGER":
                        type = DbType.Int32;
                        break;
                    case "DECIMAL":
                        type = DbType.Decimal;
                        break;
                    case "BIGINT":
                        type = DbType.Int64;
                        break;
                    case "DOUBLE":
                        type = DbType.Double;
                        break;
                    default:
                        throw new Exception("Unknown type '" + kv.Value + "'");
                }
                ret[kv.Key] = type;
            }
            return ret;
        }
        public static List<String> ReadStrings(this H2Connection connection, String query)
        {
            var ret = new List<String>();
            var reader = new H2Command(query, connection).ExecuteReader();
            while (reader.Read())
                ret.Add(reader.GetString(0));
            return ret;
        }
        public static DataTable ReadTable(this H2Connection connection, String tableName)
        {
            if (tableName == null)
                return null;
            return connection.ReadQuery("select * from \"" + tableName + "\"");
        }
        public static DataTable ReadQuery(this H2Connection connection, String query)
        {
            if (query == null)
                return null;
            var table = new DataTable()
            {
                CaseSensitive = false
            };
            new H2DataAdapter(new H2Command(query, connection)).Fill(table);
            return table;
        }
        public static String ReadString(this H2Connection connection, String query)
        {
            var result = new H2Command(query, connection).ExecuteScalar() as String;
            return result;
        }
        public static Dictionary<String, T> ReadMap<T>(this H2Connection connection, String query)
        {
            var ret = new Dictionary<String, T>();
            var reader = new H2Command(query, connection).ExecuteReader();
            while (reader.Read())
            {
                var key = reader.GetString(0);
                var value = reader.GetValue(1);
                if (value == DBNull.Value)
                    ret[key] = default(T);
                else
                    ret[key] = (T)value;
            }
            return ret;
        }
    }
	public static class CollectionExtensions
    {
        public static T[] Array<T>(params T[] a)
        {
            return a;
        }
        public static String Commas<T>(this IEnumerable<T> col)
        {
            return col.Implode(", ");
        }
        public static String Implode<T>(this IEnumerable<T> col, String sep)
        {
            return col.Where(e => e != null).Select(e => e.ToString()).Aggregate((a, b) => a + sep + b);
        }
    }
}
