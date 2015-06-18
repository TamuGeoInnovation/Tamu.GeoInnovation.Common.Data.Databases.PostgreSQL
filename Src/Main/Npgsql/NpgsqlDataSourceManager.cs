using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;
using USC.GISResearchLab.Common.Databases.DataSources;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace USC.GISResearchLab.Common.Databases.Npgsql
{
    public class NpgsqlDataSourceManager : AbstractDataSourceManager
    {
        public NpgsqlDataSourceManager()
        {
            ProviderType = DataProviderType.Npgsql;
        }

        public NpgsqlDataSourceManager(string pathToDatabaseDlls, string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = DataProviderType.Npgsql;
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDlls;
        }

        public NpgsqlDataSourceManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = DataProviderType.Npgsql;
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override void CreateDatabase(DatabaseType databaseType, string databaseName)
        {
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(DatabaseType.Npgsql, Location, DefaultDatabase, UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.Npgsql);
                QueryManager queryManager = new QueryManager(DataProviderType.Npgsql, DatabaseType.Npgsql, connectionString);

                string sql = "CREATE DATABASE " + databaseName;
                queryManager.ExecuteNonQuery(System.Data.CommandType.Text, sql);
            }
            catch (Exception ex)
            {
                string msg = "Error creating database: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }


        public override bool Validate(DatabaseType databaseType, string databaseName)
        {
            bool ret = true;
            IQueryManager queryManager = null;
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(PathToDatabaseDLLs, DatabaseType.Npgsql, Location, databaseName, UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.Npgsql);
                queryManager = new QueryManager(PathToDatabaseDLLs, DataProviderType.Npgsql, DatabaseType.Npgsql, connectionString);
                queryManager.Open();
                queryManager.Close();
            }
            catch (Exception e)
            {
                ret = false;
            }
            finally
            {
                queryManager.Dispose();
            }

            return ret;
        }
    }
}
