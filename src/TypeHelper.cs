using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Json2Parquet
{
    internal static class TypeHelper
    {
        #region Single value converters
        //public static object ToTypedObject(object o, Type type)
        //{

        //    if (o != null && (o.GetType() == type || o.GetType().IsAssignableTo(type)))
        //    {
        //        return o;
        //    }

        //    try
        //    {
        //        return new System.ComponentModel.TypeConverter().ConvertTo(o, type);
        //    }
        //    catch
        //    {
        //        return null;
        //    }
        //}

        public static bool? ToBoolean(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is bool) return (bool)o;

            return (bool)converter.ConvertTo(o, typeof(bool));
        }

        public static long? ToInt64(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is long) return (long)o;

            return (long)converter.ConvertTo(o, typeof(long));
        }

        public static int? ToInt32(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is int) return (int)o;

            return (int)converter.ConvertTo(o, typeof(int));
        }

        public static string ToString(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is string) return (string)o;

            return (string)converter.ConvertTo(o, typeof(string));
        }

        public static float? ToFloat(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is float) return (float)o;

            return (float)converter.ConvertTo(o, typeof(float));
        }

        public static double? ToDouble(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is double) return (double)o;

            return (double)converter.ConvertTo(o, typeof(double));
        }

        public static DateTime? ToDateTime(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is DateTime) return (DateTime)o;

            return (DateTime)converter.ConvertTo(o, typeof(DateTime));
        }

        public static Guid? ToGuid(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is Guid) return (Guid)o;

            return (Guid)converter.ConvertTo(o, typeof(Guid));
        }

        public static TimeSpan? ToTimeSpan(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is TimeSpan) return (TimeSpan)o;

            return (TimeSpan)converter.ConvertTo(o, typeof(TimeSpan));
        }

        public static Uri ToUri(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is Uri) return (Uri)o;

            return (Uri)converter.ConvertTo(o, typeof(Uri));
        }

        public static byte[] ToByteArray(object o, System.ComponentModel.TypeConverter converter)
        {
            if (null == o) return null;

            if (o is byte[]) return (byte[])o;

            return (byte[])converter.ConvertTo(o, typeof(byte[]));
        }
        #endregion

        #region Dictionary Converters
        public static Array ToBooleanArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<bool?>(dict, expectedRowCount, converter, ToBoolean);
        }

        public static Array ToInt64Array(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<long?>(dict, expectedRowCount, converter, ToInt64);
        }

        public static Array ToInt32Array(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<int?>(dict, expectedRowCount, converter, ToInt32);
        }

        public static Array ToStringArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<string>(dict, expectedRowCount, converter, ToString);
        }

        public static Array ToFloatArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<float?>(dict, expectedRowCount, converter, ToFloat);
        }

        public static Array ToDoubleArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<double?>(dict, expectedRowCount, converter, ToDouble);
        }

        public static Array ToDateTimeArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<DateTime?>(dict, expectedRowCount, converter, ToDateTime);
        }

        public static Array ToGuidArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<Guid?>(dict, expectedRowCount, converter, ToGuid);
        }

        public static Array ToTimeSpanArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<TimeSpan?>(dict, expectedRowCount, converter, ToTimeSpan);
        }

        public static Array ToUriArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<Uri>(dict, expectedRowCount, converter, ToUri);
        }

        public static Array ToByteArrayArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<byte[]>(dict, expectedRowCount, converter, ToByteArray);
        }

        public static Array ToObjectArray(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<object>(dict, expectedRowCount, converter, (object o, System.ComponentModel.TypeConverter tc) => { return o; });
        }


        #endregion

        #region IList Converters
        public static Array ToBooleanArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<bool?>(list, converter, ToBoolean);
        }

        public static Array ToInt64Array(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<long?>(list, converter, ToInt64);
        }

        public static Array ToInt32Array(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<int?>(list, converter, ToInt32);
            //List<int?> result = new List<int?>();
            //foreach (var o in list)
            //{
            //    result.Add(ToInt32(o, converter));
            //}

            //return result.ToArray();
        }

        public static Array ToStringArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<string>(list, converter, ToString);
        }

        public static Array ToFloatArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<float?>(list, converter, ToFloat);
        }

        public static Array ToDoubleArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<double?>(list, converter, ToDouble);
        }

        public static Array ToDateTimeArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<DateTime?>(list, converter, ToDateTime);
        }

        public static Array ToGuidArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<Guid?>(list, converter, ToGuid);
        }

        public static Array ToTimeSpanArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<TimeSpan?>(list, converter, ToTimeSpan);
        }

        public static Array ToUriArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<Uri>(list, converter, ToUri);
        }

        public static Array ToByteArrayArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<byte[]>(list, converter, ToByteArray);
        }

        public static Array ToObjectArray(System.Collections.IList list, System.ComponentModel.TypeConverter converter)
        {
            return ConvertToArray<object>(list, converter, (object o, System.ComponentModel.TypeConverter tc) => { return o; });
        }

        #endregion

        #region Helper methods
        private static Array ConvertToArray<T>(System.Collections.IList list, System.ComponentModel.TypeConverter converter, Func<object, System.ComponentModel.TypeConverter, T> handler)
        {
            List<T> result = new List<T>();
            foreach (var o in list)
            {
                result.Add(handler(o, converter));
            }

            return result.ToArray();
        }

        private static Array ConvertToArray<T>(System.Collections.IDictionary dict, int expectedRowCount, System.ComponentModel.TypeConverter converter, Func<object, System.ComponentModel.TypeConverter, T> handler)
        {
            List<T> result = new List<T>();

            for (int i = 1; i <= expectedRowCount; i++)
            {
                object value = null;
                if (dict.Contains(i))
                {
                    value = dict[i];
                }

                result.Add(handler(value, converter));
            }

            return result.ToArray();
        }
        #endregion
    }
}
