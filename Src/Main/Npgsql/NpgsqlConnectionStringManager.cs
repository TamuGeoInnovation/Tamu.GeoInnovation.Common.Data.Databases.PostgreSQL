using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;

namespace USC.GISResearchLab.Common.Databases.Npgsql
{
    public class NpgsqlConnectionStringManager : AbstractConnectionStringManager
    {
        public NpgsqlConnectionStringManager()
        {
            DatabaseType = DatabaseType.Oracle;
        }

        public NpgsqlConnectionStringManager(string pathToDatabaseDlls, string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDlls;
        }

        public NpgsqlConnectionStringManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override string GetConnectionString(DataProviderType dataProviderType)
        {
            string ret = null;
            switch (dataProviderType)
            {
                case DataProviderType.Npgsql:
                    ret = "Server=" + Location + ";Port=5432;Database=" + DefaultDatabase + ";User ID=" + UserName + ";Password=" + Password + ";"+"Pooling=false;";
                    //ret = "User ID=" + UserName + ";Password=" + Password + ";Server=" + Location + ";Port=5432;Database=" + DefaultDatabase + ";";
                        //";Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Lifetime=0;";
                    //ret = "Server=" + Location + ";Uid=" + UserName + ";Pwd=" + Password + ";Database=" + DefaultDatabase + ";";
                    break;
                case DataProviderType.Odbc:
                    ret = "";
                    break;
                case DataProviderType.OleDb:
                    ret = "";
                    break;
                default:
                    throw new Exception("Unexpected dataProviderType: " + dataProviderType);
            }
            return ret;
        }
    }
}
