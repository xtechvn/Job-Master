using Elasticsearch.Net;
using JobSyncStoreToElasticSearch.Common;
using JobSyncStoreToElasticSearch.DbWorker;
using JobSyncStoreToElasticSearch.Models;
using Nest;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Configuration;
using System.Text;

namespace JobSyncStoreToElasticSearch
{
    /// <summary>
    /// App: Xử lý các tiến trình đồng bộ data từ Store Procedure lên Elasticsearch
    /// </summary>
    class Program
    {
        private static string queue_sync_database_store_to_es = ConfigurationManager.AppSettings["sync_store_to_es"];
        public static string QUEUE_HOST = ConfigurationManager.AppSettings["QUEUE_HOST"];
        public static string QUEUE_V_HOST = ConfigurationManager.AppSettings["QUEUE_V_HOST"];
        public static string QUEUE_USERNAME = ConfigurationManager.AppSettings["QUEUE_USERNAME"];
        public static string QUEUE_PASSWORD = ConfigurationManager.AppSettings["QUEUE_PASSWORD"];
        public static string QUEUE_PORT = ConfigurationManager.AppSettings["QUEUE_PORT"];
        public static string QUEUE_KEY_API = ConfigurationManager.AppSettings["QUEUE_KEY_API"];
        public static string tele_token = ConfigurationManager.AppSettings["tele_token"];
        public static string tele_group_id = ConfigurationManager.AppSettings["tele_group_id"];
        public static string es_host_target = ConfigurationManager.AppSettings["es_host_target"];
        public static string es_document_type_target = ConfigurationManager.AppSettings["es_document_type_target"];
        
        static void Main(string[] args)
        {
            try
            {

                #region TEST
                // Kết nối tới ES
                //var nodes = new Uri[] { new Uri(es_host_target) };
                //var connectionPool = new StaticConnectionPool(nodes);
                //var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex("list_hotel_booking");
                //var elasticClient = new ElasticClient(connectionSettings);

                //// Lấy thông tin trong Database theo store name
                //var data_json = StoreDataDAL.getDataFromStore("SP_GetListHotelBooking");

                //if (!string.IsNullOrEmpty(data_json))
                //{
                //    var dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(data_json);

                //    // Đẩy data từ DB lên ElasticSearch
                //    var bulkIndexResponse = elasticClient.Bulk(b => b
                //        .Index("list_hotel_booking")
                //        .IndexMany(dataList)
                //    );
                //    // Kiểm tra kết quả trả về
                //    if (bulkIndexResponse.Errors)
                //    {
                //        foreach (var item in bulkIndexResponse.ItemsWithErrors)
                //        {
                //            Console.WriteLine($"Failed to index document {item.Id}: {item.Error}");
                //        }
                //    }
                //    else
                //    {
                //        Console.WriteLine("All documents indexed successfully.");
                //    }

                //}
                #endregion

                #region READ QUEUE
                var factory = new ConnectionFactory()
                {
                    HostName = QUEUE_HOST,
                    UserName = QUEUE_USERNAME,
                    Password = QUEUE_PASSWORD,
                    VirtualHost = QUEUE_V_HOST,
                    Port = Protocols.DefaultProtocol.DefaultPort
                };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    try
                    {
                        channel.QueueDeclare(queue: queue_sync_database_store_to_es,
                                             durable: true,
                                             exclusive: false,
                                             autoDelete: false,
                                             arguments: null);

                        channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                        Console.WriteLine(" [*] Waiting for messages.");

                        var consumer = new EventingBasicConsumer(channel);
                        consumer.Received += (sender, ea) =>
                        {
                            try
                            {

                                var body = ea.Body.ToArray();
                                var message = Encoding.UTF8.GetString(body);

                                var obj_data = JsonConvert.DeserializeObject<DataInfoModel>(message);

                                Console.WriteLine("Receivice Data:" + message);

                                //1. Kết nối tới ES
                                var nodes = new Uri[] { new Uri(es_host_target) };
                                var connectionPool = new StaticConnectionPool(nodes);
                                var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex(obj_data.index_es);
                                var elasticClient = new ElasticClient(connectionSettings);

                                //2. Lấy thông tin trong Database theo store name
                                var data_json = StoreDataDAL.getDataFromStore(obj_data.store_name);

                                if (!string.IsNullOrEmpty(data_json))
                                {
                                    var dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(data_json);

                                    //3. Đẩy data từ DB lên ElasticSearch
                                    var bulkIndexResponse = elasticClient.Bulk(b => b
                                        .Index(obj_data.index_es)
                                        .IndexMany(dataList)
                                    );
                                    // Kiểm tra kết quả trả về
                                    if (bulkIndexResponse.Errors)
                                    {
                                        foreach (var item in bulkIndexResponse.ItemsWithErrors)
                                        {
                                            Console.WriteLine($"Failed to index document {item.Id}: {item.Error}");
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine("All documents indexed successfully.");
                                    }

                                }

                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("error queue: " + ex.ToString());
                                ErrorWriter.InsertLogTelegramByUrl(tele_token, tele_group_id, "error queue = " + ex.ToString());
                            }
                        };

                        channel.BasicConsume(queue: queue_sync_database_store_to_es, autoAck: false, consumer: consumer);

                        Console.ReadLine();

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        // ErrorWriter.InsertLogTelegramByUrl(tele_token, tele_group_id, "error queue = " + ex.ToString());
                    }
                }
                #endregion

            }
            catch (Exception ex)
            {
                //ErrorWriter.InsertLogTelegramByUrl(tele_token, tele_group_id, "Main (JOB APP_PUSH) => error queue = " + ex.ToString());
                Console.WriteLine(" [x] Received message: {0}", ex.ToString());
            }
        }

    }
}