using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;

namespace JobSyncStoreToElasticSearch.DbWorker.Adavigo_Travel
{
    class AdavigoTravelRepository: BaseDataRepository
    {
        public AdavigoTravelRepository(string _connection, string _store_name) : base(_connection, _store_name) { }

        public virtual string getListHotelBooking(string store_name)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[16];
                objParam[0] = new SqlParameter("@ServiceCode", DBNull.Value);
                objParam[1] = new SqlParameter("@OrderCode", DBNull.Value);
                objParam[2] = new SqlParameter("@StatusBooking", DBNull.Value);
                objParam[3] = new SqlParameter("@CheckinDateFrom", DBNull.Value);
                objParam[4] = new SqlParameter("@CheckinDateTo", DBNull.Value);
                objParam[5] = new SqlParameter("@CheckoutDateFrom", DBNull.Value);
                objParam[6] = new SqlParameter("@CheckoutDateTo", DBNull.Value);

                objParam[7] = new SqlParameter("@UserCreate", DBNull.Value);
                objParam[8] = new SqlParameter("@CreateDateFrom", DBNull.Value);

                objParam[9] = new SqlParameter("@CreateDateTo", DBNull.Value);

                objParam[10] = new SqlParameter("@SalerId", DBNull.Value);
                objParam[11] = new SqlParameter("@OperatorId", DBNull.Value);
                objParam[12] = new SqlParameter("@PageIndex", -1);
                objParam[13] = new SqlParameter("@PageSize", -1);
                objParam[14] = new SqlParameter("@SalerPermission", DBNull.Value);
                objParam[15] = new SqlParameter("@BookingCode", DBNull.Value);

                db_worker.Fill(obj_table_result, store_name, objParam);
                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                //LogHelper.InsertLogTelegram("GetPagingList - ContractDAL: " + ex);
            }
            return null;
        }

    }
}
