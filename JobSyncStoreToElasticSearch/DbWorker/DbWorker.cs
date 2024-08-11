using JobSyncStoreToElasticSearch.Common;
using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace JobSyncStoreToElasticSearch.DbWorker
{
    public class DbWorker
    {
        private static string _connection = ConfigurationManager.AppSettings["database_source"];
        private static string startupPath = AppDomain.CurrentDomain.BaseDirectory;
        public static void Fill(DataTable dataTable, string procedureName, SqlParameter[] parameters)
        {
            using (SqlConnection oConnection = new SqlConnection(_connection))
            {
                SqlCommand oCommand = new SqlCommand(procedureName, oConnection);
                oCommand.CommandType = CommandType.StoredProcedure;

                if (parameters != null)
                {
                    oCommand.Parameters.AddRange(parameters);
                }
                SqlDataAdapter oAdapter = new SqlDataAdapter();
                oAdapter.SelectCommand = oCommand;
                oConnection.Open();

                using (SqlTransaction oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        oAdapter.SelectCommand.Transaction = oTransaction;
                        oAdapter.Fill(dataTable);
                        oTransaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param " + ex.ToString());
                        oTransaction.Rollback();
                        throw;
                    }
                    finally
                    {
                        if (oConnection.State == ConnectionState.Open)
                        {
                            oConnection.Close();
                        }
                        oConnection.Dispose();
                        oAdapter.Dispose();
                    }
                }
                if (oConnection.State != ConnectionState.Closed)
                    ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param", oConnection.State.ToString());
            }
        }



    }
}