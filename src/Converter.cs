using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Json2Parquet
{
    public static class Converter
    {

        public static async Task ToParquet(string source, string destination)
        {
            if (!System.IO.File.Exists(source)) throw new System.IO.FileNotFoundException("File does not exists or you don't have access to the file", source);

            if (System.IO.File.Exists(destination)) System.IO.File.Delete(destination);

            if (false == System.IO.Directory.Exists(System.IO.Path.GetDirectoryName(destination))) System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(destination));

            using (var src = System.IO.File.OpenRead(source))
            {
                using (var dst = System.IO.File.Create(destination))
                {
                    await ToParquet(src, dst);

                    await dst.FlushAsync();
                    dst.Close();
                }
                src.Close();
            }
        }

        public static async Task ToParquet(System.IO.Stream source, System.IO.Stream destination)
        {
            //parquet api requires that we pre-build the schema as well as columns containing values

            //if we treat everything as string datatype, parsing can become significantly simpler and less error prone

            Dictionary<string, Parquet.Data.DataColumn> columns = new Dictionary<string, Parquet.Data.DataColumn>();
            Dictionary<string, Type> columnTypes = new Dictionary<string, Type>();
            Dictionary<string, System.Collections.IDictionary> columnData = new Dictionary<string, System.Collections.IDictionary>();

            Dictionary<string, Parquet.Data.Field> schemaFields = new Dictionary<string, Parquet.Data.Field>();
            //SortedList<int, string> columnIndex = new SortedList<int, string>();
            List<string> columnIndex = new List<string>();

            int totalRowsFound = 0;
            using (var src = new System.IO.StreamReader(source))
            {
                string line = await src.ReadLineAsync();
                int rowNumber = 0;
                while (false == string.IsNullOrWhiteSpace(line))
                {
                    rowNumber++;

                    //parse line, build schema elements, build column elements, set columnindex if needed
                    var entity = Newtonsoft.Json.JsonConvert.DeserializeObject<Newtonsoft.Json.Linq.JObject>(line);

                    if (null == entity) continue; //nothing was parsed ???

                    Traverse(entity, rowNumber, schemaFields, columnTypes, columnData, columnIndex);

                    line = await src.ReadLineAsync();
                }

                totalRowsFound = rowNumber;
            }

            if (totalRowsFound > 0)
            {
                List<Parquet.Data.Field> fields = new List<Parquet.Data.Field>();
                //foreach (string columnName in columnIndex.Values)
                foreach (string columnName in columnIndex)
                {
                    if (false == schemaFields.ContainsKey(columnName)) continue; //should not happen
                    if (null == schemaFields[columnName]) continue; //should not happen


                    if (false == columnTypes.ContainsKey(columnName)) continue; //should not happen
                    if (null == columnTypes[columnName]) continue; //should not happen

                    if (false == columnData.ContainsKey(columnName)) continue; //should not happen
                    if (null == columnData[columnName]) continue; //should not happen

                    fields.Add(schemaFields[columnName]);

                    //need columnType and data arrays -- 
                    if (false == columns.ContainsKey(columnName))
                    {
                        columns.Add(columnName, new Parquet.Data.DataColumn(new Parquet.Data.DataField(columnName, columnTypes[columnName]), columnData[columnName].ToArray(columnTypes[columnName], totalRowsFound)));
                    }
                }

                var schema = new Parquet.Data.Schema(fields);

                using (var writer = new Parquet.ParquetWriter(schema, destination, new Parquet.ParquetOptions() { TreatByteArrayAsString = true }))
                {
                    using (var group = writer.CreateRowGroup())
                    {
                        foreach (string columnName in columnIndex)
                        {
                            if (false == columns.ContainsKey(columnName)) continue; //should not happen
                            if (null == columns[columnName]) continue; //should not happen

                            group.WriteColumn(columns[columnName]);
                        }
                    }
                }
            }
        }


        private static void Traverse(Newtonsoft.Json.Linq.JObject token, int rowNumber, Dictionary<string, Parquet.Data.Field> schema, Dictionary<string, Type> columnTypes, Dictionary<string, System.Collections.IDictionary> columnData, List<string> columnIndex)
        {
            var columnName = $"{token.Path}";
            foreach (var t in token)
            {
                Traverse(t.Value, rowNumber, schema, columnTypes, columnData, columnIndex);
            }
        }

        private static void Traverse(Newtonsoft.Json.Linq.JArray token, int rowNumber, Dictionary<string, Parquet.Data.Field> schema, Dictionary<string, Type> columnTypes, Dictionary<string, System.Collections.IDictionary> columnData, List<string> columnIndex)
        {
            var columnName = $"{token.Path}";
            foreach (var t in token)
            {
                Traverse(t, rowNumber, schema, columnTypes, columnData, columnIndex);
            }
        }

        private static void Traverse(Newtonsoft.Json.Linq.JToken token, int rowNumber, Dictionary<string, Parquet.Data.Field> schema, Dictionary<string, Type> columnTypes, Dictionary<string, System.Collections.IDictionary> columnData, List<string> columnIndex)
        {

            if (null == token) return; //nothing was parsed ???


            if (token is Newtonsoft.Json.Linq.JValue)
            {
                Traverse(token as Newtonsoft.Json.Linq.JValue, rowNumber, schema, columnTypes, columnData, columnIndex);
                return;
            }

            if (token is Newtonsoft.Json.Linq.JProperty)
            {
                Traverse(token as Newtonsoft.Json.Linq.JProperty, rowNumber, schema, columnTypes, columnData, columnIndex);
                return;
            }

            if (token is Newtonsoft.Json.Linq.JArray)
            {
                Traverse(token as Newtonsoft.Json.Linq.JArray, rowNumber, schema, columnTypes, columnData, columnIndex);
                return;
            }

            if (token is Newtonsoft.Json.Linq.JObject)
            {
                Traverse(token as Newtonsoft.Json.Linq.JObject, rowNumber, schema, columnTypes, columnData, columnIndex);
                return;
            }

        }


        private static void Traverse(Newtonsoft.Json.Linq.JValue token, int rowNumber, Dictionary<string, Parquet.Data.Field> schema, Dictionary<string, Type> columnTypes, Dictionary<string, System.Collections.IDictionary> columnData, List<string> columnIndex)
        {
            if (null == token) return;

            var columnName = $"{token.Path}";

            Type columnType = null;
            System.Collections.IDictionary list = null;

            switch (token.Type)
            {
                case Newtonsoft.Json.Linq.JTokenType.Boolean:
                    columnType = typeof(bool?);
                    list = new Dictionary<int, bool?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Bytes:
                    columnType = typeof(byte[]);
                    list = new Dictionary<int, byte[]>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Date:
                    columnType = typeof(DateTime?);
                    list = new Dictionary<int, DateTime?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Float:
                    columnType = typeof(double?);
                    list = new Dictionary<int, double?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Guid:
                    columnType = typeof(Guid?);
                    list = new Dictionary<int, Guid?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Integer:
                    columnType = typeof(long?);
                    list = new Dictionary<int, long?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.String:
                    columnType = typeof(string);
                    list = new Dictionary<int, string>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.TimeSpan:
                    columnType = typeof(TimeSpan?);
                    list = new Dictionary<int, TimeSpan?>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Uri:
                    columnType = typeof(Uri);
                    list = new Dictionary<int, Uri>();
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Null:
                    //what should we do if parser sees this as "NULL"?
                    break;
                case Newtonsoft.Json.Linq.JTokenType.Array:
                case Newtonsoft.Json.Linq.JTokenType.Object:
                case Newtonsoft.Json.Linq.JTokenType.Property:
                    //should not possible as this should've been caught by prior 'more generic' traverse methods
                    break;
                default:
                    break;
            }

            if (null == columnType) return;

            //t.
            object columnValue = token.Value; //todo: read from t
                                              //Type columnType = columnValue.GetType();

            if (false == columnTypes.ContainsKey(columnName))
            {
                columnTypes.Add(columnName, columnType);
            }

            if (false == columnData.ContainsKey(columnName))
            {
                columnData.Add(columnName, list);
            }

            if (false == schema.ContainsKey(columnName))
            {
                schema.Add(columnName, new Parquet.Data.DataField(columnName, columnType));
            }

            if (false == columnIndex.Contains(columnName))
            {
                //int idx = 0;
                //if (false == string.IsNullOrWhiteSpace(previous))
                //{
                //    if (false == columnIndex.Contains(previous))
                //    {
                //        columnIndex.Add(previous); //should this go at the end ?
                //    }

                //    idx = columnIndex.IndexOf(previous);
                //}

                //idx++;
                //if (idx >= columnIndex.Count)
                //{
                //    columnIndex.Add(columnName);
                //}
                //else
                //{
                //    columnIndex.Insert(idx , columnName);
                //}

                columnIndex.Add(columnName);
            }


            try
            {

                columnData[columnName].Add(rowNumber, columnValue);
                //columnData[columnName].Add(rowNumber, TypeHelper.ToTypedObject(columnValue, columnType));
            }
            catch
            {
                //  throw; this typically happens when an object type "changes" from row to row... if row one value was 12345 -- long and row 2 value was 4332.2332 float --> how should this be done?
            }

        }

    }




    public static class MyExtensions
    {
        public static Array ToArray(this System.Collections.IDictionary dict, Type type, int expectedRowCount)
        {
            System.ComponentModel.TypeConverter tc = new System.ComponentModel.TypeConverter();

            if (type == typeof(bool) || type == typeof(bool?)) return TypeHelper.ToBooleanArray(dict, expectedRowCount, tc);

            if (type == typeof(int) || type == typeof(int?)) return TypeHelper.ToInt32Array(dict, expectedRowCount, tc);

            if (type == typeof(long) || type == typeof(long?)) return TypeHelper.ToInt64Array(dict, expectedRowCount, tc);

            if (type == typeof(float) || type == typeof(float?)) return TypeHelper.ToFloatArray(dict, expectedRowCount, tc);

            if (type == typeof(double) || type == typeof(double?)) return TypeHelper.ToDoubleArray(dict, expectedRowCount, tc);

            if (type == typeof(string)) return TypeHelper.ToStringArray(dict, expectedRowCount, tc);

            if (type == typeof(DateTime?) || type == typeof(DateTime?)) return TypeHelper.ToDateTimeArray(dict, expectedRowCount, tc);

            if (type == typeof(TimeSpan) || type == typeof(TimeSpan?)) return TypeHelper.ToTimeSpanArray(dict, expectedRowCount, tc);

            if (type == typeof(Guid) || type == typeof(Guid?)) return TypeHelper.ToGuidArray(dict, expectedRowCount, tc);

            if (type == typeof(Uri)) return TypeHelper.ToUriArray(dict, expectedRowCount, tc);

            if (type == typeof(byte[])) return TypeHelper.ToByteArrayArray(dict, expectedRowCount, tc);

            return TypeHelper.ToObjectArray(dict, expectedRowCount, tc);
        }

    }

}
