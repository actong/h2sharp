#region MIT License
/*
 * Copyright © 2008 Jonathan Mark Porter.
 * H2Sharp is a wrapper for the H2 Database Engine. http://h2sharp.googlecode.com
 * 
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#endregion
using java.sql;
using System.Collections.Generic;
namespace System.Data.H2
{
    static class H2Helper
    {
		static Dictionary<int, DbType> jdbc2dbtype;
		static Dictionary<DbType, int> dbtype2jdbc;
		static void map(int jdbcType, DbType dbType) {
			try {
				jdbc2dbtype[jdbcType] = dbType;
				dbtype2jdbc[dbType] = jdbcType;
			} catch (Exception) {}
		}
		static H2Helper() {
			jdbc2dbtype = new Dictionary<int, DbType>();
			dbtype2jdbc = new Dictionary<DbType, int>();
			map(Types.VARCHAR, DbType.AnsiString);
			map(Types.CHAR, DbType.AnsiStringFixedLength);
			map(Types.BINARY, DbType.Binary);
			map(Types.BOOLEAN, DbType.Boolean);
			map(Types.TINYINT, DbType.Byte);
			map(Types.DATE, DbType.Date);
			map(Types.TIMESTAMP, DbType.DateTime);
			map(Types.TIMESTAMP, DbType.DateTime2);
			map(Types.TIMESTAMP, DbType.DateTimeOffset);
			map(Types.DECIMAL, DbType.Decimal);
			map(Types.DOUBLE, DbType.Double);
			map(Types.SMALLINT, DbType.Int16);
			map(Types.INTEGER, DbType.Int32);
			map(Types.BIGINT, DbType.Int64);
			map(Types.JAVA_OBJECT, DbType.Object);
			map(Types.TINYINT, DbType.SByte);
			map(Types.FLOAT, DbType.Single);
			map(Types.NVARCHAR, DbType.String);
			map(Types.NCHAR, DbType.StringFixedLength);
			map(Types.TIME, DbType.Time);
			map(Types.ARRAY, DbType.VarNumeric);
			//DbType.Guid:
			//DbType.UInt16:
			//DbType.UInt32:
			//DbType.UInt64:
			//DbType.Currency:
		}
		public static int GetTypeCode(DbType dbType)
		{
			int ret;
			if (!dbtype2jdbc.TryGetValue(dbType, out ret))
				throw new NotSupportedException("Cannot convert the ADO.NET " + Enum.GetName(typeof(DbType), dbType) + " " + typeof(DbType).Name + " to a JDBC type");
			
			return ret;
        }
        
        public static DbType GetDbType(int typeCode)
		{
			DbType ret;
			if (!jdbc2dbtype.TryGetValue(typeCode, out ret))
				throw new NotSupportedException("Cannot convert JDBC type " + typeCode + " to an ADO.NET " + typeof(DbType).Name);
			
			return ret;
        }

        public static IsolationLevel GetAdoTransactionLevel(int level)
        {
            switch (level)
            {
                case java.sql.Connection.__Fields.TRANSACTION_NONE:
                    return IsolationLevel.Unspecified;
                case java.sql.Connection.__Fields.TRANSACTION_READ_COMMITTED:
                    return IsolationLevel.ReadCommitted;
                case java.sql.Connection.__Fields.TRANSACTION_READ_UNCOMMITTED:
                    return IsolationLevel.ReadUncommitted;
                case java.sql.Connection.__Fields.TRANSACTION_REPEATABLE_READ:
                    return IsolationLevel.RepeatableRead;
                case java.sql.Connection.__Fields.TRANSACTION_SERIALIZABLE:
                    return IsolationLevel.Serializable;
                default:
                    throw new NotSupportedException("unsupported transaction level");
            }
        }
        public static int GetJdbcTransactionLevel(IsolationLevel level)
        {
            switch (level)
            {
                case IsolationLevel.Unspecified:
                    return java.sql.Connection.__Fields.TRANSACTION_NONE;
                case IsolationLevel.ReadCommitted:
                    return java.sql.Connection.__Fields.TRANSACTION_READ_COMMITTED;
                case IsolationLevel.ReadUncommitted:
                    return java.sql.Connection.__Fields.TRANSACTION_READ_UNCOMMITTED;
                case IsolationLevel.RepeatableRead:
                    return java.sql.Connection.__Fields.TRANSACTION_REPEATABLE_READ;
                case IsolationLevel.Serializable:
                    return java.sql.Connection.__Fields.TRANSACTION_SERIALIZABLE;
                default:
                    throw new NotSupportedException("unsupported transaction level");
            }
        }

        public delegate Object Converter(Object o);

        public static Object ConvertToDotNet(Object result)
        {
            if (result == null)
                return DBNull.Value;

            return ConverterToDotNet(result)(result);
        }
        public static Converter ConverterToDotNet(Object resultSample)
        {
            if (resultSample == null || resultSample is DBNull)
                return null;

            if (resultSample is java.lang.Integer)
                return result => ((java.lang.Integer)result).intValue();

            if (resultSample is java.lang.Long)
                return result => ((java.lang.Long)result).longValue();

            if (resultSample is java.lang.Short)
                return result => ((java.lang.Short)result).shortValue();

            if (resultSample is java.lang.Character)
                return result => ((java.lang.Character)result).charValue();

            if (resultSample is java.lang.Byte)
                return result => ((java.lang.Byte)result).byteValue();

            if (resultSample is java.lang.Short)
                return result => ((java.lang.Short)result).shortValue();

            if (resultSample.GetType() == typeof(java.lang.String))
                return result => result.ToString();

            if (resultSample is java.lang.Number)
                return result => ((java.lang.Number)result).intValue();
            
            if (resultSample is java.sql.Date)
                return result => UTCStart.AddMilliseconds(((java.sql.Date)result).getTime());
            
            if (resultSample is java.sql.Timestamp)
                return result => UTCStart.AddMilliseconds(((java.sql.Timestamp)result).getTime());

            return result => result;
        }
        static readonly DateTime UTCStart = new DateTime(1970, 1, 1);
        public static Converter ConverterToJava(object resultSample)
        {
            if (resultSample is int)
                return result => new java.lang.Integer((int)result);

            if (resultSample is long)
                return result => new java.lang.Long((long)result);

            if (resultSample is short)
                return result => new java.lang.Short((short)result);

            if (resultSample is byte)
                return result => new java.lang.Byte((byte)result);

            if (resultSample is double)
                return result => new java.lang.Double((double)result);

            if (resultSample is float)
                return result => new java.lang.Float((float)result);

            if (resultSample is char)
                return result => new java.lang.Character((char)result);

            if (resultSample is short)
                return result => new java.lang.Short((short)result);

            if (resultSample is DateTime)
                return result => new java.sql.Timestamp((long)(((DateTime)resultSample) - UTCStart).TotalMilliseconds);

            return result => result;
        }

        public static Type GetType(int typeCode)
        {
            switch (typeCode)
            {
                case 0:
                    return typeof(DBNull);
                case -2:
                case 2004:
                case -4:
                case -3:
                    return typeof(byte[]);
                case -1:
                case 12:
                    return typeof(string);
                case 4:
                    return typeof(int);
                case 3:
                    return typeof(decimal);
                case 1:
                    return typeof(char);
                case 6:
                    return typeof(float);
                case 8:
                    return typeof(double);
                case -5:
                    return typeof(long);
                case 5:
                    return typeof(short);
                default:
                    return null;
            }
        }
    }
}