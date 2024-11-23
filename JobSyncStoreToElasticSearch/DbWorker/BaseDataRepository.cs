using Newtonsoft.Json;
using System.Data;
using System.Data.SqlClient;

namespace JobSyncStoreToElasticSearch.DbWorker
{
    /// <summary>
    ///Áp dụng quy tắc SOLID: Liskov để định nghĩa 1 lớp PARENT trừu tượng là lớp xử lý các hàm chung được dùng lại trên nhiều database
    /// ///Lớp này là lớp Base và k dc phép sửa đổi
    /// </summary>
    /// 
    public class BaseDataRepository
    {
        public string connection { get; set; }
        public string store_name { get; set; }
        public BaseDataRepository(string _connection, string _store_name)
        {
            connection = _connection;
            store_name = _store_name;
        }


        /// <summary>
        /// project_type: dựa vào đây để lấy ra các chuỗi connection sql tương ứng
        /// </summary>
        /// <param name="store_name"></param>
        /// <param name="id"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        public virtual string getAllArticle(int id)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[1];
                objParam[0] = new SqlParameter("@article_id", id);

                db_worker.Fill(obj_table_result, store_name, objParam);

                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public virtual string getAllGroupProduct(int id)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[1];
                objParam[0] = new SqlParameter("@GroupProductId", id);
                db_worker.Fill(obj_table_result, store_name, objParam);
                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        public virtual string getAllAccountAccess(int id)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[1];
                objParam[0] = new SqlParameter("@account_id", id);
                db_worker.Fill(obj_table_result, store_name, objParam);
                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public virtual string GetDataById(int id)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[1];
                objParam[0] = new SqlParameter("@Dataid", id);

                db_worker.Fill(obj_table_result, store_name, objParam);

                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public virtual string GetDataByIdAdavigo(int id)
        {
            try
            {
                var db_worker = new DbWorker(connection);
                var obj_table_result = new DataTable();
                SqlParameter[] objParam = new SqlParameter[1];
                objParam[0] = new SqlParameter("@DataID", id);

                db_worker.Fill(obj_table_result, store_name, objParam);

                if (obj_table_result.Rows.Count > 0)
                {
                    return JsonConvert.SerializeObject(obj_table_result, Formatting.Indented);
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }





    }
}
