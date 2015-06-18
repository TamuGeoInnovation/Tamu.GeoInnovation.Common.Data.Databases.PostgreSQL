using System;
using NpgsqlTypes;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.TypeConverters;

namespace USC.GISResearchLab.Common.Databases.Npgsql
{
    public class NpgsqlTypeConverter : AbstractDatabaseDataProviderTypeConverterManager
    {

        #region TypeNames
        public static string TYPENAME_BigInt = "BigInt";
        public static string TYPENAME_Binary = "Binary";
        public static string TYPENAME_Bit = "Bit";
        public static string TYPENAME_Blob = "Blob";
        public static string TYPENAME_Byte = "Byte";
        public static string TYPENAME_Char = "Char";
        public static string TYPENAME_Date = "Date";
        public static string TYPENAME_Datetime = "Datetime";
        public static string TYPENAME_Decimal = "Decimal";
        public static string TYPENAME_Double = "Double Precision";
        public static string TYPENAME_Enum = "Enum";
        public static string TYPENAME_Float = "Float";
        public static string TYPENAME_Geometry = "Geometry";
        public static string TYPENAME_Geography = "Geography";
        public static string TYPENAME_UserDefined = "USER-DEFINED";
        public static string TYPENAME_Character_Varying = "character varying";
        public static string TYPENAME_Int = "Int";
        public static string TYPENAME_Int16 = "Int16";
        public static string TYPENAME_Int24 = "Int24";
        public static string TYPENAME_Int32 = "Int32";
        public static string TYPENAME_Int64 = "Int64";
        public static string TYPENAME_LongText = "LongText";
        public static string TYPENAME_LongBlob = "LongBlob";
        public static string TYPENAME_MediumBlob = "MediumBlob";
        public static string TYPENAME_MediumText = "MediumText";
        public static string TYPENAME_Newdate = "Newdate";
        public static string TYPENAME_NewDecimal = "NewDecimal";
        public static string TYPENAME_Set = "Set";
        public static string TYPENAME_String = "String";
        public static string TYPENAME_Text = "Text";
        public static string TYPENAME_Time = "Time";
        public static string TYPENAME_Timestamp = "Timestamp";
        public static string TYPENAME_TinyBlob = "TinyBlob";
        public static string TYPENAME_TinyText = "TinyText";
        public static string TYPENAME_UByte = "UByte";
        public static string TYPENAME_UInt16 = "UInt16";
        public static string TYPENAME_UInt24 = "UInt24";
        public static string TYPENAME_UInt32 = "UInt32";
        public static string TYPENAME_UInt64 = "UInt64";
        public static string TYPENAME_VarBinary = "VarBinary";
        public static string TYPENAME_VarChar = "VarChar";
        public static string TYPENAME_VarString = "VarString";
        public static string TYPENAME_Year = "Year";
        #endregion

        public NpgsqlTypeConverter()
        {
            DatabaseType = DatabaseType.Npgsql;
            DataProviderType = DataProviderType.Npgsql;
        }

        public override int GetDefaultLength(DatabaseSuperDataType type)
        {
            throw new NotImplementedException();
        }


        public override int GetDefaultPrecision(DatabaseSuperDataType type)
        {
            throw new NotImplementedException();
        }

        public override DatabaseSuperDataType ToSuperType(object dbType)
        {
            DatabaseSuperDataType ret;
            NpgsqlDbType type = (NpgsqlDbType)dbType;

            switch (type)
            {
                case NpgsqlDbType.Bytea:
                    ret = DatabaseSuperDataType.Binary;
                    break;
                case NpgsqlDbType.Bit:
                    ret = DatabaseSuperDataType.Bit;
                    break;
               /* case NpgsqlDbType.Bytea:
                    ret = DatabaseSuperDataType.Blob;
                    break;*/
                case NpgsqlDbType.Date:
                    ret = DatabaseSuperDataType.Date;
                    break;
                case NpgsqlDbType.Timestamp:
                    ret = DatabaseSuperDataType.DateTime;
                    break;
                case NpgsqlDbType.Numeric:
                    ret = DatabaseSuperDataType.Decimal;
                    break;
                case NpgsqlDbType.Double:
                    ret = DatabaseSuperDataType.Float;
                    break;
                //case NpgsqlDbType.Geometry:
                //    ret = DatabaseSuperDataType.Geometry;
                //    break;
                case NpgsqlDbType.Smallint:
                    ret = DatabaseSuperDataType.Int16;
                    break;
                case NpgsqlDbType.Integer:
                    ret = DatabaseSuperDataType.Int24;
                    break;
             /*   case NpgsqlDbType.Integer:
                    ret = DatabaseSuperDataType.Int32;
                    break;*/
                case NpgsqlDbType.Bigint:
                    ret = DatabaseSuperDataType.Int64;
                    break;
              /*  case NpgsqlDbType.Bytea:
                    ret = DatabaseSuperDataType.MediumBlob;
                    break;*/
                //case NpgsqlDbType.Newdate:
                //    ret = DatabaseSuperDataType.Newdate;
                //    break;
                //case NpgsqlDbType.NewDecimal:
                //    ret = DatabaseSuperDataType.NewDecimal;
                //    break;
                //case NpgsqlDbType.Set:
                //    ret = DatabaseSuperDataType.Set;
                //    break;
                case NpgsqlDbType.Text:
                    ret = DatabaseSuperDataType.String;
                    break;
             /*   case NpgsqlDbType.Text:
                    ret = DatabaseSuperDataType.Text;
                    break;*/
                case NpgsqlDbType.Time:
                    ret = DatabaseSuperDataType.Time;
                    break;
               /* case NpgsqlDbType.Timestamp:
                    ret = DatabaseSuperDataType.Timestamp;
                    break;*/
             /*   case NpgsqlDbType.Bytea:
                    ret = DatabaseSuperDataType.TinyBlob;
                    break;*/
                case NpgsqlDbType.Varchar:
                    ret = DatabaseSuperDataType.TinyText;
                    break;
                //case NpgsqlDbType.UByte:
                //    ret = DatabaseSuperDataType.UByte;
                //    break;
              /*  case NpgsqlDbType.Integer:
                    ret = DatabaseSuperDataType.UInt16;
                    break;*/
              /*  case NpgsqlDbType.Bigint:
                    ret = DatabaseSuperDataType.UInt24;
                    break;*/
               /* case NpgsqlDbType.Bigint:
                    ret = DatabaseSuperDataType.UInt32;
                    break;*/
               /* case NpgsqlDbType.Numeric:
                    ret = DatabaseSuperDataType.UInt64;
                    break;*/
               /* case NpgsqlDbType.Bytea:
                    ret = DatabaseSuperDataType.VarBinary;
                    break;*/
                /*case NpgsqlDbType.Varchar:
                    ret = DatabaseSuperDataType.VarChar;
                    break;*/
              /*  case NpgsqlDbType.Text:
                    ret = DatabaseSuperDataType.VarString;
                    break;*/
                //case NpgsqlDbType.Year:
                //    ret = DatabaseSuperDataType.Year;
                //    break;
                default:
                    throw new Exception("Unexpected type: " + type);
            }
            return ret;
        }


        public NpgsqlDbType FromShowColumnsString(string type)
        {
            NpgsqlDbType ret = NpgsqlDbType.Text;

            if (String.Compare(type, TYPENAME_BigInt, true) == 0)
            {
                ret = NpgsqlDbType.Numeric;
            }
            else if (String.Compare(type, TYPENAME_Binary, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_Bit, true) == 0)
            {
                ret = NpgsqlDbType.Bit;
            }
            else if (String.Compare(type, TYPENAME_Blob, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_Byte, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_Char, true) == 0)
            {
                ret = NpgsqlDbType.Varchar;
            }
            else if (String.Compare(type, TYPENAME_Character_Varying, true) == 0)
            {
                ret = NpgsqlDbType.Varchar;
            }
            else if (String.Compare(type, TYPENAME_Date, true) == 0)
            {
                ret = NpgsqlDbType.Date;
            }
            else if (String.Compare(type, TYPENAME_Datetime, true) == 0)
            {
                ret = NpgsqlDbType.Timestamp;
            }
            else if (String.Compare(type, TYPENAME_Decimal, true) == 0)
            {
                ret = NpgsqlDbType.Numeric;
            }
            else if (String.Compare(type, TYPENAME_Double, true) == 0)
            {
                ret = NpgsqlDbType.Double;
            }
            //else if (String.Compare(type, TYPENAME_Enum, true) == 0)
            //{
            //    ret = NpgsqlDbType.Enum;
            //}
            else if (String.Compare(type, TYPENAME_Float, true) == 0)
            {
                ret = NpgsqlDbType.Double;
            }
            else if (String.Compare(type, TYPENAME_Geography, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_Geometry, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_UserDefined, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_Int, true) == 0)
            {
                ret = NpgsqlDbType.Integer;
            }
            else if (String.Compare(type, TYPENAME_Int16, true) == 0)
            {
                ret = NpgsqlDbType.Smallint;
            }
            else if (String.Compare(type, TYPENAME_Int24, true) == 0)
            {
                ret = NpgsqlDbType.Integer;
            }
            else if (String.Compare(type, TYPENAME_Int32, true) == 0)
            {
                ret = NpgsqlDbType.Integer;
            }
            else if (String.Compare(type, TYPENAME_Int64, true) == 0)
            {
                ret = NpgsqlDbType.Bigint;
            }
            else if (String.Compare(type, TYPENAME_LongBlob, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_LongText, true) == 0)
            {
                ret = NpgsqlDbType.Text;
            }
            else if (String.Compare(type, TYPENAME_MediumBlob, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_MediumText, true) == 0)
            {
                ret = NpgsqlDbType.Varchar;
            }
            //else if (String.Compare(type, TYPENAME_Newdate, true) == 0)
            //{
            //    ret = NpgsqlDbType.Newdate;
            //}
            //else if (String.Compare(type, TYPENAME_NewDecimal, true) == 0)
            //{
            //    ret = NpgsqlDbType.NewDecimal;
            //}
            //else if (String.Compare(type, TYPENAME_Set, true) == 0)
            //{
            //    ret = NpgsqlDbType.Set;
            //}
            else if (String.Compare(type, TYPENAME_String, true) == 0)
            {
                ret = NpgsqlDbType.Text;
            }
            else if (String.Compare(type, TYPENAME_Text, true) == 0)
            {
                ret = NpgsqlDbType.Text;
            }
            else if (String.Compare(type, TYPENAME_Time, true) == 0)
            {
                ret = NpgsqlDbType.Time;
            }
            else if (String.Compare(type, TYPENAME_Timestamp, true) == 0)
            {
                ret = NpgsqlDbType.Timestamp;
            }
            else if (String.Compare(type, TYPENAME_TinyBlob, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_TinyText, true) == 0)
            {
                ret = NpgsqlDbType.Varchar;
            }
            //else if (String.Compare(type, TYPENAME_UByte, true) == 0)
            //{
            //    ret = NpgsqlDbType.UByte;
            //}
            else if (String.Compare(type, TYPENAME_UInt16, true) == 0)
            {
                ret = NpgsqlDbType.Integer;
            }
            else if (String.Compare(type, TYPENAME_UInt24, true) == 0)
            {
                ret = NpgsqlDbType.Bigint;
            }
            else if (String.Compare(type, TYPENAME_UInt32, true) == 0)
            {
                ret = NpgsqlDbType.Bigint;
            }
            else if (String.Compare(type, TYPENAME_UInt64, true) == 0)
            {
                ret = NpgsqlDbType.Numeric;
            }
            else if (String.Compare(type, TYPENAME_VarBinary, true) == 0)
            {
                ret = NpgsqlDbType.Bytea;
            }
            else if (String.Compare(type, TYPENAME_VarChar, true) == 0)
            {
                ret = NpgsqlDbType.Varchar;
            }
            else if (String.Compare(type, TYPENAME_VarString, true) == 0)
            {
                ret = NpgsqlDbType.Text;
            }
            //else if (String.Compare(type, TYPENAME_Year, true) == 0)
            //{
            //    ret = NpgsqlDbType.Year;
            //}
            else
            {
                throw new Exception("Unexpected or unimplemented FromShowColumnsString: " + type);
            }

            return ret;
        }

        public override object FromDatabaseSuperDataType(DatabaseSuperDataType type)
        {
            NpgsqlDbType ret = NpgsqlDbType.Text;

            switch (type)
            {
                case DatabaseSuperDataType.BigInt:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.Binary:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Bit:
                    ret = NpgsqlDbType.Bit;
                    break;
                case DatabaseSuperDataType.Blob:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Boolean:
                    ret = NpgsqlDbType.Bit;
                    break;
                case DatabaseSuperDataType.BSTR:
                    ret = NpgsqlDbType.Varchar;
                    break;
                case DatabaseSuperDataType.Char:
                    ret = NpgsqlDbType.Varchar;
                    break;
                case DatabaseSuperDataType.Counter:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.Currency:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.Date:
                    ret = NpgsqlDbType.Date;
                    break;
                case DatabaseSuperDataType.DateTime:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.DateTime2:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.DateTimeOffset:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.DBDate:
                    ret = NpgsqlDbType.Date;
                    break;
                case DatabaseSuperDataType.DBTime:
                    ret = NpgsqlDbType.Time;
                    break;
                case DatabaseSuperDataType.DBTimeStamp:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.Decimal:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.Double:
                    ret = NpgsqlDbType.Double;
                    break;
                case DatabaseSuperDataType.Empty:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Error:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Filetime:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.Float:
                    ret = NpgsqlDbType.Double;
                    break;
                case DatabaseSuperDataType.Geography:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Geometry:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Guid:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.IDispatch:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Image:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Int16:
                    ret = NpgsqlDbType.Smallint;
                    break;
                case DatabaseSuperDataType.Int24:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.Int32:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.Int64:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.IUnknown:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Long:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.LongBinary:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.LongText:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.LongVarBinary:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.LongVarChar:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.LongVarWChar:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.MediumBlob:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.Money:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.NChar:
                    ret = NpgsqlDbType.Varchar;
                    break;
                //case DatabaseSuperDataType.Newdate:
                //    ret = NpgsqlDbType.Newdate;
                //    break;
                //case DatabaseSuperDataType.NewDecimal:
                //    ret = NpgsqlDbType.NewDecimal;
                //    break;
                case DatabaseSuperDataType.NText:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Numeric:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.NVarChar:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.PropVariant:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Real:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.Set:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Short:
                    ret = NpgsqlDbType.Smallint;
                    break;
                case DatabaseSuperDataType.Single:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.SmallDateTime:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.SmallInt:
                    ret = NpgsqlDbType.Smallint;
                    break;
                case DatabaseSuperDataType.SmallMoney:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.String:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Structured:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Text:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Time:
                    ret = NpgsqlDbType.Time;
                    break;
                case DatabaseSuperDataType.Timestamp:
                    ret = NpgsqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.TinyBlob:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.TinyInt:
                    ret = NpgsqlDbType.Smallint;
                    break;
                case DatabaseSuperDataType.TinyText:
                    ret = NpgsqlDbType.Varchar;
                    break;
                //case DatabaseSuperDataType.UByte:
                //    ret = NpgsqlDbType.UByte;
                //    break;
                case DatabaseSuperDataType.Udt:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.UInt16:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.UInt24:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.UInt32:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.UInt64:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.UniqueIdentifier:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.UnsignedBigInt:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.UnsignedInt:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.UnsignedSmallInt:
                    ret = NpgsqlDbType.Bigint;
                    break;
                case DatabaseSuperDataType.UnsignedTinyInt:
                    ret = NpgsqlDbType.Integer;
                    break;
                case DatabaseSuperDataType.VarBinary:
                    ret = NpgsqlDbType.Bytea;
                    break;
                case DatabaseSuperDataType.VarChar:
                    ret = NpgsqlDbType.Varchar;
                    break;
                case DatabaseSuperDataType.Variant:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.VarNumeric:
                    ret = NpgsqlDbType.Numeric;
                    break;
                case DatabaseSuperDataType.VarString:
                    ret = NpgsqlDbType.Text;
                    break;
                case DatabaseSuperDataType.VarWChar:
                    ret = NpgsqlDbType.Varchar;
                    break;
                case DatabaseSuperDataType.WChar:
                    ret = NpgsqlDbType.Varchar;
                    break;
                case DatabaseSuperDataType.Xml:
                    ret = NpgsqlDbType.Text;
                    break;
                //case DatabaseSuperDataType.Year:
                //    ret = NpgsqlDbType.Year;
                //    break;
                default:
                    throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            }

            return ret;
        }

        public override Type GetSystemType(object dbType)
        {
            Type ret = null;
            NpgsqlDbType type = (NpgsqlDbType)dbType;

            switch (type)
            {
                case NpgsqlDbType.Bytea:
                    ret = typeof(Byte[]);
                    break;
                case NpgsqlDbType.Bit:
                    ret = typeof(Boolean);
                    break;
              /*  case NpgsqlDbType.Bytea:
                    ret = typeof(Byte[]);
                    break;*/
                case NpgsqlDbType.Date:
                    ret = typeof(DateTime);
                    break;
                case NpgsqlDbType.Timestamp:
                    ret = typeof(DateTime);
                    break;
                case NpgsqlDbType.Numeric:
                    ret = typeof(Decimal);
                    break;
                case NpgsqlDbType.Double:
                    ret = typeof(Double);
                    break;
                //case NpgsqlDbType.Geometry:
                //    ret = typeof(String);
                //    break;
                case NpgsqlDbType.Smallint:
                    ret = typeof(Int16);
                    break;
                case NpgsqlDbType.Integer:
                    ret = typeof(Int32);
                    break;
              /*  case NpgsqlDbType.Integer:
                    ret = typeof(Int32);
                    break;*/
                case NpgsqlDbType.Bigint:
                    ret = typeof(Int64);
                    break;
                /*case NpgsqlDbType.Bytea:
                    ret = typeof(Byte[]);
                    break; */
                //case NpgsqlDbType.Newdate:
                //    ret = typeof(DateTime);
                //    break;
                //case NpgsqlDbType.NewDecimal:
                //    ret = typeof(Decimal);
                //    break;
                //case NpgsqlDbType.Set:
                //    ret = typeof(Object[]);
                //    break;
                case NpgsqlDbType.Text:
                    ret = typeof(String);
                    break;
               /* case NpgsqlDbType.Text:
                    ret = typeof(String);
                    break;*/
               /* case NpgsqlDbType.Time:
                    ret = typeof(DateTime);
                    break;*/
               /* case NpgsqlDbType.Timestamp:
                    ret = typeof(DateTime);
                    break;*/
               /* case NpgsqlDbType.Bytea:
                    ret = typeof(Byte[]);
                    break;*/
                case NpgsqlDbType.Varchar:
                    ret = typeof(String);
                    break;
                //case NpgsqlDbType.UByte:
                //    ret = typeof(Byte);
                //    break;
               /* case NpgsqlDbType.Integer:
                    ret = typeof(UInt16);
                    break;*/
              /*  case NpgsqlDbType.Bigint:
                    ret = typeof(UInt32);
                    break;*/
               /* case NpgsqlDbType.Bigint:
                    ret = typeof(UInt32);
                    break;*/
               /* case NpgsqlDbType.Numeric:
                    ret = typeof(UInt64);
                    break;*/
                /*case NpgsqlDbType.Bytea:
                    ret = typeof(Byte[]);
                    break;*/
               /* case NpgsqlDbType.Varchar:
                    ret = typeof(String);
                    break;*/
                /*case NpgsqlDbType.Text:
                    ret = typeof(String);
                    break; */
                //case NpgsqlDbType.Year:
                //    ret = typeof(Int32);
                //    break;
                default:
                    throw new Exception("Unexpected type: " + type);
            }
            return ret;
        }

        public override object ConvertType(object dbSuperType, DatabaseType databaseType)
        {
            throw new NotImplementedException();
        }

        public override string GetTypeAsString(DatabaseSuperDataType type)
        {
            string ret = null;

            switch (type)
            {
                case DatabaseSuperDataType.BigInt:
                    ret = TYPENAME_Int64;
                    break;
                case DatabaseSuperDataType.Binary:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.Bit:
                    ret = TYPENAME_Bit;
                    break;
                case DatabaseSuperDataType.Blob:
                    ret = TYPENAME_Blob;
                    break;
                case DatabaseSuperDataType.Boolean:
                    ret = TYPENAME_Bit;
                    break;
                case DatabaseSuperDataType.BSTR:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Char:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Counter:
                    ret = TYPENAME_Int32;
                    break;
                case DatabaseSuperDataType.Currency:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.Date:
                    ret = TYPENAME_Date;
                    break;
                case DatabaseSuperDataType.DateTime:
                    ret = TYPENAME_Datetime;
                    break;
                case DatabaseSuperDataType.DateTime2:
                    ret = TYPENAME_Datetime;
                    break;
                case DatabaseSuperDataType.DateTimeOffset:
                    ret = TYPENAME_Datetime;
                    break;
                case DatabaseSuperDataType.DBDate:
                    ret = TYPENAME_Date;
                    break;
                case DatabaseSuperDataType.DBTime:
                    ret = TYPENAME_Time;
                    break;
                case DatabaseSuperDataType.DBTimeStamp:
                    ret = TYPENAME_Timestamp;
                    break;
                case DatabaseSuperDataType.Decimal:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.Double:
                    ret = TYPENAME_Double;
                    break;
                case DatabaseSuperDataType.Empty:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Error:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Filetime:
                    ret = TYPENAME_Datetime;
                    break;
                case DatabaseSuperDataType.Float:
                    ret = TYPENAME_Float;
                    break;
                case DatabaseSuperDataType.Geography:
                    ret = TYPENAME_Geography;
                    break;
                case DatabaseSuperDataType.Geometry:
                    ret = TYPENAME_Geometry;
                    break;
                case DatabaseSuperDataType.Guid:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.IDispatch:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Image:
                    ret = TYPENAME_Blob;
                    break;
                case DatabaseSuperDataType.Int16:
                    ret = TYPENAME_Int16;
                    break;
                case DatabaseSuperDataType.Int24:
                    ret = TYPENAME_Int24;
                    break;
                case DatabaseSuperDataType.Int32:
                    ret = TYPENAME_Int32;
                    break;
                case DatabaseSuperDataType.Int64:
                    ret = TYPENAME_Int64;
                    break;
                case DatabaseSuperDataType.IUnknown:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Long:
                    ret = TYPENAME_Int32;
                    break;
                case DatabaseSuperDataType.LongBinary:
                    ret = TYPENAME_Blob;
                    break;
                case DatabaseSuperDataType.LongText:
                    ret = TYPENAME_LongText;
                    break;
                case DatabaseSuperDataType.LongVarBinary:
                    ret = TYPENAME_LongBlob;
                    break;
                case DatabaseSuperDataType.LongVarChar:
                    ret = TYPENAME_LongText;
                    break;
                case DatabaseSuperDataType.LongVarWChar:
                    ret = TYPENAME_LongText;
                    break;
                case DatabaseSuperDataType.MediumBlob:
                    ret = TYPENAME_MediumBlob;
                    break;
                case DatabaseSuperDataType.Money:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.NChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Newdate:
                    ret = TYPENAME_Newdate;
                    break;
                case DatabaseSuperDataType.NewDecimal:
                    ret = TYPENAME_NewDecimal;
                    break;
                case DatabaseSuperDataType.NText:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Numeric:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.NVarChar:
                    ret = TYPENAME_VarString;
                    break;
                case DatabaseSuperDataType.PropVariant:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Real:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.Set:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Short:
                    ret = TYPENAME_Int16;
                    break;
                case DatabaseSuperDataType.Single:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.SmallDateTime:
                    ret = TYPENAME_Datetime;
                    break;
                case DatabaseSuperDataType.SmallInt:
                    ret = TYPENAME_Int16;
                    break;
                case DatabaseSuperDataType.SmallMoney:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.String:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Structured:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Text:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Time:
                    ret = TYPENAME_Time;
                    break;
                case DatabaseSuperDataType.Timestamp:
                    ret = TYPENAME_Timestamp;
                    break;
                case DatabaseSuperDataType.TinyBlob:
                    ret = TYPENAME_TinyBlob;
                    break;
                case DatabaseSuperDataType.TinyInt:
                    ret = TYPENAME_Int16;
                    break;
                case DatabaseSuperDataType.TinyText:
                    ret = TYPENAME_TinyText;
                    break;
                case DatabaseSuperDataType.UByte:
                    ret = TYPENAME_UByte;
                    break;
                case DatabaseSuperDataType.Udt:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.UInt16:
                    ret = TYPENAME_UInt16;
                    break;
                case DatabaseSuperDataType.UInt24:
                    ret = TYPENAME_UInt24;
                    break;
                case DatabaseSuperDataType.UInt32:
                    ret = TYPENAME_UInt32;
                    break;
                case DatabaseSuperDataType.UInt64:
                    ret = TYPENAME_UInt64;
                    break;
                case DatabaseSuperDataType.UniqueIdentifier:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.UnsignedBigInt:
                    ret = TYPENAME_UInt64;
                    break;
                case DatabaseSuperDataType.UnsignedInt:
                    ret = TYPENAME_UInt32;
                    break;
                case DatabaseSuperDataType.UnsignedSmallInt:
                    ret = TYPENAME_UInt24;
                    break;
                case DatabaseSuperDataType.UnsignedTinyInt:
                    ret = TYPENAME_UInt16;
                    break;
                case DatabaseSuperDataType.VarBinary:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.VarChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Variant:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.VarNumeric:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.VarString:
                    ret = TYPENAME_VarString;
                    break;
                case DatabaseSuperDataType.VarWChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.WChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Xml:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Year:
                    ret = TYPENAME_Year;
                    break;
                default:
                    throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            }

            return ret;
        }

        public override Type ToSystemTypeFromDbTypeString(string dbTypeString)
        {
            throw new NotImplementedException("See SqlServerTypeConverter for example implementation");
        }

        public override DatabaseSuperDataType ToSuperTypeFromdbTypeString(string dbTypeString)
        {
            throw new NotImplementedException("See SqlServerTypeConverter for example implementation");
        }
    }
}
