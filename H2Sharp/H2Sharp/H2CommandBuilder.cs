using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.H2;
using System.Text.RegularExpressions;
using System.Data;

namespace Supplix.Utils.H2
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
}
