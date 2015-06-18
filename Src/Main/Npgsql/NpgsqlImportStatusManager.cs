using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using Npgsql;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ImportStatusManagers;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.StoredProcedures;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Utils.Databases;
using System.Text;

namespace USC.GISResearchLab.Common.Databases.Npgsql
{
    public class NpgsqlImportStatusManager : AbstractImportStatusManager
    {
       

        //#region Properties


        //public TraceSource TraceSource { get; set; }
        //public string ApplicationConnectionString { get; set; }
        //public DataProviderType ApplicationDataProviderType { get; set; }
        //public DatabaseType ApplicationDatabaseType { get; set; }
        //public string ApplicationPathToDatabaseDlls { get; set; }

        private IQueryManager _QueryManager;
        public IQueryManager QueryManager
        {
            get
            {
                if (_QueryManager == null)
                {
                    _QueryManager = new QueryManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
                }
                return _QueryManager;
            }
        }

        //private ISchemaManager _SchemaManager;
        //public ISchemaManager SchemaManager
        //{
        //    get
        //    {
        //        if (_SchemaManager == null)
        //        {
        //            _SchemaManager = new SchemaManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
        //        }
        //        return _SchemaManager;
        //    }
        //}

        //#endregion

        public NpgsqlImportStatusManager(TraceSource traceSource)
        {
            TraceSource = traceSource;
        }


        public NpgsqlImportStatusManager(DataProviderType providerType)
        {
            ProviderType = providerType;
        }

        public NpgsqlImportStatusManager(DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public NpgsqlImportStatusManager(string pathToDatabaseDLLs, DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDLLs;
        }

        public NpgsqlImportStatusManager(string pathToDatabaseDlls, DataProviderType providerType, string connectionString)
        {
            SchemaManager = SchemaManagerFactory.GetSchemaManager(pathToDatabaseDlls, providerType, connectionString);
        }
        

        public override void InitializeConnections()
        {

        }

        public override void CreateStoredProcedures(bool shouldThrowExceptions)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();

                ForiegnKeyRemover foriegnKeyRemover = new ForiegnKeyRemover(SchemaManager.QueryManager.Connection.Database);
                string dropForeignKeysDropSql = foriegnKeyRemover.GetDropSQL();
                string dropForeignKeysCreateSql = foriegnKeyRemover.GetCreateSQL();

                SchemaManager.AddStoredProcedureToDatabase(dropForeignKeysDropSql, false);
                SchemaManager.AddStoredProcedureToDatabase(dropForeignKeysCreateSql, false);
            }
            catch (Exception e)
            {
                string msg = "Error CreateStoredProcedures: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                if (shouldThrowExceptions)
                {
                    throw new Exception(msg, e);
                }
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusStateTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();
                
                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

               // string sql = "\\c " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                StringBuilder sb = new StringBuilder();
                sb.Append("CREATE TABLE " + tableName + " (");
                sb.Append("id BIGSERIAL NOT NULL,");
                sb.Append("state Varchar DEFAULT NULL,");
                sb.Append("status Varchar DEFAULT NULL,");
                sb.Append("startDate TimeStamp DEFAULT NULL,");
                sb.Append("endDate Timestamp DEFAULT NULL,");
                sb.Append("message Varchar DEFAULT NULL,");
                sb.Append("PRIMARY KEY  (id)");
                sb.Append(");");


                SchemaManager.AddTableToDatabase(tableName, sb.ToString());
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusStateTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusCountyTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();

                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

                //string sql = "\\c " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                StringBuilder sb = new StringBuilder();
                sb.Append("CREATE TABLE " + tableName + " (");
                sb.Append("id BIGSERIAL NOT NULL,");
                sb.Append("state Varchar DEFAULT NULL,");
                sb.Append("county Varchar DEFAULT NULL,");
                sb.Append("status Varchar DEFAULT NULL,");
                sb.Append("startDate Timestamp DEFAULT NULL,");
                sb.Append("endDate Timestamp DEFAULT NULL,");
                sb.Append("message Varchar DEFAULT NULL,");
                sb.Append("PRIMARY KEY  (id)");
                sb.Append(");");

                SchemaManager.AddTableToDatabase(tableName, sb.ToString());
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusCountyTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusFileTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();

                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

                //string sql = "\\c " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                StringBuilder sb = new StringBuilder();
                sb.Append("CREATE TABLE " + tableName + " (");
                sb.Append("id BIGSERIAL NOT NULL,");
                sb.Append("state Varchar DEFAULT NULL,");
                sb.Append("county Varchar DEFAULT NULL,");
                sb.Append("filename Varchar DEFAULT NULL,");
                sb.Append("status Varchar DEFAULT NULL,");
                sb.Append("startDate Timestamp DEFAULT NULL,");
                sb.Append("endDate Timestamp DEFAULT NULL,");
                sb.Append("message Varchar DEFAULT NULL,");
                sb.Append("PRIMARY KEY  (id)");
                sb.Append(");");

                SchemaManager.AddTableToDatabase(tableName, sb.ToString(), false);
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusFileTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override bool CheckStatusStateAlreadyDone(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking state status: " + state);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" state=@state");
                sb.Append(" and ");
                sb.Append(" status='Finished'");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking state status: " + state;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusCountyAlreadyDone(string tableName, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking county status: " + county);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" county=@county");
                sb.Append(" and ");
                sb.Append(" status='Finished'");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking county status: " + county;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusFileAlreadyDone(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking file status: " + file);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" filename=@filename");
                sb.Append(" and ");
                sb.Append(" state=@state");
                sb.Append(" and ");
                sb.Append(" county=@county");
                sb.Append(" and ");
                sb.Append(" status='Finished'");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking file status: " + file;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        

        public override bool UpdateStatusFile(string tableName, string state, string county, string file, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating file status: " + file + " status: " + status);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" filename=@filename");
                sb.Append(" and ");
                sb.Append(" county=@county");
                sb.Append(" and ");
                sb.Append(" state=@state");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusFile(tableName, state, county,  file);
                }
                else
                {
                    //sql = "update " + tableName;
                    sb.Clear();
                    sb.Append("update " + tableName);
                    sb.Append(" set ");
                    sb.Append(" status=@status,");
                    sb.Append(" endDate=@endDate,");
                    sb.Append(" message=@message");
                    sb.Append(" where ");
                    sb.Append(" id=@id ");

                    cmd = new SqlCommand(sb.ToString());
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating file status: " + file + " status: " + status;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }

        

        public override bool UpdateStatusState(string tableName, string state, Statuses status, string message)
        {
            bool ret = false;

            try
            {
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating state status: " + state + " status: " + status);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" state=@state");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusState(tableName, state);
                }
                else
                {
                    //sql = "update " + tableName + "";
                    sb.Clear();
                    sb.Append("update " + tableName + "");
                    sb.Append(" set ");
                    sb.Append(" status=@status,");
                    sb.Append(" endDate=@endDate,");
                    sb.Append(" message=@message");
                    sb.Append(" where ");
                    sb.Append(" id=@id ");

                    cmd = new SqlCommand(sb.ToString());
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating state status: " + state + " status: " + status + ":" + exc.Message;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }

       

        public override bool UpdateStatusCounty(string tableName, string state, string county, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating county status: " + county + " status: " + status);
                }

                //string sql = "select id FROM " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("select id FROM " + tableName + "");
                sb.Append(" where ");
                sb.Append(" state=@state");
                sb.Append(" and ");
                sb.Append(" county=@county");

                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusCounty(tableName, state, county);
                }
                else
                {
                    //sql = "update " + tableName + "";
                    sb.Clear();
                    sb.Append("update " + tableName + "");
                    sb.Append(" set ");
                    sb.Append(" status=@status,");
                    sb.Append(" endDate=@endDate,");
                    sb.Append(" message=@message");
                    sb.Append(" where ");
                    sb.Append(" id=@id ");

                    cmd = new SqlCommand(sb.ToString());
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating county status: " + county + " status: " + status + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusFile(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting file status: " + file + " status: ");
                }

                //string sql = "INSERT into " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("INSERT into " + tableName + "");
                sb.Append(" (");
                sb.Append(" state,");
                sb.Append(" county,");
                sb.Append(" filename,");
                sb.Append(" status,");
                sb.Append(" startDate");
                sb.Append(" )");
                sb.Append(" VALUES ");
                sb.Append(" (");
                sb.Append(" @state,");
                sb.Append(" @county,");
                sb.Append(" @filename,");
                sb.Append(" @status,");
                sb.Append(" @startDate");
                sb.Append(" )");


                IDbCommand cmd = new NpgsqlCommand(sb.ToString());
                cmd.Parameters.Add(NpgsqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(NpgsqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(NpgsqlParameterUtils.BuildSqlParameter("filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(NpgsqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(NpgsqlParameterUtils.BuildSqlParameter("startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting file status: " + file + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusState(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting state status: " + state + " status: ");
                }

                //string sql = "INSERT into " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("INSERT into " + tableName + "");
                sb.Append(" (");
                sb.Append(" state,");
                sb.Append(" status,");
                sb.Append(" startDate");
                sb.Append(" )");
                sb.Append(" VALUES ");
                sb.Append(" (");
                sb.Append(" @state,");
                sb.Append(" @status,");
                sb.Append(" @startDate");
                sb.Append(" )");


                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting state status: " + state + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusCounty(string tableName, string state, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting county status: " + county + " status: ");
                }

                //string sql = "INSERT into " + tableName + "";
                StringBuilder sb = new StringBuilder();
                sb.Append("INSERT into " + tableName + "");
                sb.Append(" (");
                sb.Append(" state,");
                sb.Append(" county,");
                sb.Append(" status,");
                sb.Append(" startDate");
                sb.Append(" )");
                sb.Append(" VALUES ");
                sb.Append(" (");
                sb.Append(" @state,");
                sb.Append(" @county,");
                sb.Append(" @status,");
                sb.Append(" @startDate");
                sb.Append(" )");


                SqlCommand cmd = new SqlCommand(sb.ToString());
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting county status: " + county + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }
    }
}
