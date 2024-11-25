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
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                           
                            case "SP_GetGroupProduct":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetAccountClient":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetClient":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetOrder":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetAccountAccessApi":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetArticle":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetArticleCategory":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetArticleRelated":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetTag":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetLocationProduct":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetUser":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetAddressClient":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetProvince":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetDistrict":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetWard":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetOrderDetail":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetRating":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetAccountAccessAPIPermission":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "sp_getOrderDetail":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetArticleTagData":
                                json_data_source = data_hulotoys.GetDataById(Convert.ToInt32(obj_data.id));
                                break;

                            default:
                                break;
                        }

                        break;
                    case ProjectType.ADAVIGO_CMS:
                        connection_source = ConfigurationManager.AppSettings["database_adavigo"].ToString(); // Chuỗi connect tới Database                             
                        es_host_target = ConfigurationManager.AppSettings["es_master"].ToString();  // dia chi es để tranfer data

                        // Connect lấy data
                        var data_adavigo = new BiolifeRepository(connection_source, obj_data.store_name);

                        switch (obj_data.store_name)
                        {
                            case "sp_GetPrograms":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;
                            case "sp_GetUser":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetClient":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetOrder":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetHotel":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetHotelBooking":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetNational":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "SP_GetDetailFlyBookingDetail":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            case "sp_GetArticle":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            //case "sp_GetTour":
                            //    json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                            //    break;

                            case "sp_GetHotelBookingCode":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;
                            case "SP_GetContract":
                                json_data_source = data_adavigo.GetDataByIdAdavigo(Convert.ToInt32(obj_data.id));
                                break;

                            default:
                                break;
                        }


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
