using System;
using System.Data;
using Npgsql;
using USC.GISResearchLab.Common.Core.Databases.BulkCopys;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace USC.GISResearchLab.Common.Core.Databases.Npgsql
{
    public class NpgsqlBulkCopy : AbstractBulkCopy
    {
        public NpgsqlBulkCopy(DatabaseType databaseType, NpgsqlConnection conn)
        {
            DatabaseType = databaseType;
            Connection = conn;
            QueryManager = new QueryManager(DataProviderType.OleDb, DatabaseType, Connection.ConnectionString);
        }

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void GenerateColumnMappings()
        {
            throw new NotImplementedException();
        }

        public override void GenerateColumnMappings(string[] excludeColumns)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(DataRow[] rows)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(DataTable dataTable)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(DataTable dataTable, DataRowState dataRowState)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(IDataReader dataReader)
        {
            throw new NotImplementedException();
        }
    }
}
