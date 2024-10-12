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
        //public static void Fill(DataTable dataTable, string procedureName, SqlParameter[] parameters)
        //{
        //    using (SqlConnection oConnection = new SqlConnection(_connection))
        //    {
        //        SqlCommand oCommand = new SqlCommand(procedureName, oConnection);
        //        oCommand.CommandType = CommandType.StoredProcedure;

        //        if (parameters != null)
        //        {
        //            oCommand.Parameters.AddRange(parameters);
        //        }
        //        SqlDataAdapter oAdapter = new SqlDataAdapter();
        //        oAdapter.SelectCommand = oCommand;
        //        oConnection.Open();

        //        using (SqlTransaction oTransaction = oConnection.BeginTransaction())
        //        {
        //            try
        //            {
        //                oAdapter.SelectCommand.Transaction = oTransaction;
        //                oAdapter.Fill(dataTable);
        //                oTransaction.Commit();
        //            }
        //            catch (Exception ex)
        //            {
        //                ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param " + ex.ToString());
        //                oTransaction.Rollback();
        //                throw;
        //            }
        //            finally
        //            {
        //                if (oConnection.State == ConnectionState.Open)
        //                {
        //                    oConnection.Close();
        //                }
        //                oConnection.Dispose();
        //                oAdapter.Dispose();
        //            }
        //        }
        //        if (oConnection.State != ConnectionState.Closed)
        //            ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param", oConnection.State.ToString());
        //    }
        //}

        //public static DataSet ExecuteQueryNotParam(string procedureName, int rp = -1)
        //{
        //    using (SqlConnection oConnection = new SqlConnection(_connection))
        //    {
        //        SqlCommand oCommand = new SqlCommand(procedureName, oConnection);
        //        oCommand.CommandType = CommandType.StoredProcedure;
        //        DataSet oReturnValue = new DataSet();
        //        oConnection.Open();

        //        using (SqlTransaction oTransaction = oConnection.BeginTransaction())
        //        {
        //            try
        //            {
        //                oCommand.Transaction = oTransaction;
        //                Fill(oReturnValue, procedureName, rp);
        //                oTransaction.Commit();
        //            }
        //            catch (Exception ex)
        //            {
        //                ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param " + ex.ToString());
        //                oTransaction.Rollback();
        //                throw;
        //            }
        //            finally
        //            {
        //                if (oConnection.State == ConnectionState.Open)
        //                {
        //                    oConnection.Close();
        //                }
        //                oConnection.Dispose();
        //                oCommand.Dispose();

        //            }
        //        }
        //        if (oConnection.State != ConnectionState.Closed)
        //            // LogsWriter.WriteLog(HttpContext.Current.Server.MapPath("~"), "Procedure ExecuteQuery", oConnection.State.ToString());
        //            ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param", oConnection.State.ToString());
        //        return oReturnValue;
        //    }
        //}

        public static void Fill(DataSet dataSet, string procedureName, SqlConnection sqlConnection, int rp = -1)
        {
            SqlCommand oCommand = new SqlCommand(procedureName, sqlConnection);
            oCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter oAdapter = new SqlDataAdapter();
            oAdapter.SelectCommand = oCommand;

            using (SqlTransaction oTransaction = sqlConnection.BeginTransaction())
            {
                try
                {
                    oAdapter.SelectCommand.Transaction = oTransaction;
                    oAdapter.Fill(dataSet);
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
                    oAdapter.Dispose();
                }
            }
            if (sqlConnection.State != ConnectionState.Closed)
            {
                ErrorWriter.WriteLog(startupPath, "Procedure Fill Data Table with param", sqlConnection.State.ToString());
            }
        }


    }
}