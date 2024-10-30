using JobSyncStoreToElasticSearch.Constant;
using JobSyncStoreToElasticSearch.DbWorker.Biolife;
using JobSyncStoreToElasticSearch.DbWorker.Hulotoys;
using JobSyncStoreToElasticSearch.Models;
using Newtonsoft.Json;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace JobSyncStoreToElasticSearch.DbWorker
{
    public class StoreDataDAL
    {
        public static DataConfigResultModel getDataFromStore(DataInfoModel obj_data)
        {
            try
            {
                string connection_source = string.Empty;
                string json_data_source = string.Empty;
                string node_es_target = string.Empty;
                string es_host_target = string.Empty; // dia chi es
                switch (obj_data.project_type)
                {
                    case ProjectType.BIOLIFE:
                        connection_source = ConfigurationManager.AppSettings["database_bio_life"].ToString(); // Chuỗi connect tới Database                             
                        es_host_target = ConfigurationManager.AppSettings["es_master"].ToString();  // dia chi es để tranfer data

                        // Connect lấy data
                        var data_biolife = new BiolifeRepository(connection_source, obj_data.store_name);

                        switch (obj_data.store_name)
                        {
                            case "SP_GetAllArticle":
                                json_data_source = data_biolife.getAllArticle(Convert.ToInt32(obj_data.id));
                                break;
                            case "sp_getGroupProduct":
                                json_data_source = data_biolife.getAllGroupProduct(Convert.ToInt32(obj_data.id));
                                break;
                            case "sp_GetAccountAccess":
                                json_data_source = data_biolife.getAllAccountAccess(Convert.ToInt32(obj_data.id));
                                break;
                            default:
                                break;
                        }


                        break;
                    case ProjectType.HULOTOYS:
                        connection_source = ConfigurationManager.AppSettings["database_hulotoys"].ToString(); // Chuỗi connect tới Database                             
                        es_host_target = ConfigurationManager.AppSettings["es_master"].ToString();  // dia chi es để tranfer data

                        // Connect lấy data
                        var data_hulotoys = new HulotoysRepository(connection_source, obj_data.store_name);
                        switch (obj_data.store_name)
                        {
                            case "SP_GetAllArticle":
                                json_data_source = data_hulotoys.getAllArticle(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetProvinces":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetDistricts":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetWards":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;

                            default:
                                break;
                        }

                        break;

                    case ProjectType.ADAVIGO_CMS:

                        break;

                    case ProjectType.HOANBDS:
                        connection_source = ConfigurationManager.AppSettings["database_hoanbds"].ToString(); // Chuỗi connect tới Database                             
                        es_host_target = ConfigurationManager.AppSettings["es_master"].ToString();  // dia chi es để tranfer data

                        // Connect lấy data
                        var data_biolife2 = new BiolifeRepository(connection_source, obj_data.store_name);

                        switch (obj_data.store_name)
                        {
                            case "SP_GetAllArticle":
                                json_data_source = data_biolife2.getAllArticle(Convert.ToInt32(obj_data.id));
                                break;
                            case "sp_getGroupProduct":
                                json_data_source = data_biolife2.getAllGroupProduct(Convert.ToInt32(obj_data.id));
                                break;
                            //case "sp_GetAccountAccess":
                            //    json_data_source = data_biolife2.getAllAccountAccess(Convert.ToInt32(obj_data.id));
                            //    break;
                            default:
                                break;
                        }

                        break;
                    default:
                        break;
                }

                var model = new DataConfigResultModel
                {
                    data_source = json_data_source,
                    index_node = obj_data.index_es,
                    es_host_target = es_host_target
                };
                return model;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

    }
}
