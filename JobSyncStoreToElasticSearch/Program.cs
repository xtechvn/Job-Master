using Elasticsearch.Net;
using JobSyncStoreToElasticSearch.Common;
using JobSyncStoreToElasticSearch.Constant;
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
        public static string QUEUE_NAME = ConfigurationManager.AppSettings["QUEUE_NAME"];
        public static string QUEUE_HOST = ConfigurationManager.AppSettings["QUEUE_HOST"];
        public static string QUEUE_V_HOST = ConfigurationManager.AppSettings["QUEUE_V_HOST"];
        public static string QUEUE_USERNAME = ConfigurationManager.AppSettings["QUEUE_USERNAME"];
        public static string QUEUE_PASSWORD = ConfigurationManager.AppSettings["QUEUE_PASSWORD"];
        public static string QUEUE_PORT = ConfigurationManager.AppSettings["QUEUE_PORT"];
        

        public static string tele_token = ConfigurationManager.AppSettings["tele_token"];
        public static string tele_group_id = ConfigurationManager.AppSettings["tele_group_id"];

        // Địa chỉ es để database sync lên
        public static string es_host_target = ConfigurationManager.AppSettings["es_master"];


        static void Main(string[] args)
        {
            try
            {
                #region TEST
              //  var obj_data_queue = new DataInfoModel
              //  {
              //      index_es = "es_biolife_sp_get_article",
              //      project_type = Convert.ToInt16(ProjectType.BIOLIFE),
              //      store_name = "SP_GetAllArticle",
              //      id = "-1"
              //  };

              //  var obj_config_info = StoreDataDAL.getDataFromStore(obj_data_queue);

              //  // Kết nối tới ES
              //  var nodes = new Uri[] { new Uri(obj_config_info.es_host_target) };
              //  var connectionPool = new StaticConnectionPool(nodes);
              //  var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex(obj_data_queue.index_es);
              //  var elasticClient = new ElasticClient(connectionSettings);

              //  // Lấy thông tin trong Database theo store name               
              //  var dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(obj_config_info.data_source);

              //  var bulkIndexResponse = elasticClient.Bulk(b => b
              //    .Index(obj_config_info.index_node)
              //    .IndexMany(dataList, (descriptor, item) =>
              //    {
              //        // Lấy id từ từ điển
              //        object idValue;
              //        if (item.TryGetValue("id", out idValue))  // Giả sử id được lưu với khóa "id"
              //        {
              //            return descriptor.Id(idValue.ToString());
              //        }
              //        return descriptor;  // Nếu không có id, nó sẽ bỏ qua
              //    })
              //);



              //  // Kiểm tra kết quả trả về
              //  if (bulkIndexResponse.Errors)
              //  {
              //      foreach (var item in bulkIndexResponse.ItemsWithErrors)
              //      {
              //          Console.WriteLine($"Failed to index document {item.Id}: {item.Error}");
              //      }
              //  }
              //  else
              //  {
              //      Console.WriteLine("All documents indexed successfully.");
              //  }


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
                        channel.QueueDeclare(queue: QUEUE_NAME,
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

                                Console.WriteLine("Receivice Data:" + message);

                                var obj_data_queue = JsonConvert.DeserializeObject<DataInfoModel>(message);

                                //1. Lấy kết quả từ database đổ về
                                var obj_config_info = StoreDataDAL.getDataFromStore(obj_data_queue);


                                //2. Kết nối tới ES
                                var nodes = new Uri[] { new Uri(obj_config_info.es_host_target) };
                                var connectionPool = new StaticConnectionPool(nodes);
                                var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex(obj_config_info.index_node);
                                var elasticClient = new ElasticClient(connectionSettings);

                                if (obj_config_info != null)
                                {
                                    var dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(obj_config_info.data_source);
                                    var projectTypeName = ProjectType.GetProjectTypeName(obj_data_queue.project_type);

                                    //3. Đẩy data từ DB lên ElasticSearch
                                    var bulkIndexResponse = elasticClient.Bulk(b => b
                                      .Index(obj_config_info.index_node)
                                      .IndexMany(dataList, (descriptor, item) =>
                                      {
                                          // Lấy id từ từ điển
                                          object idValue;
                                          if (item.TryGetValue("id", out idValue))  // Giả sử id được lưu với khóa "id"
                                          {
                                              item.Add("project_type", projectTypeName);
                                              return descriptor.Id(idValue.ToString());
                                          }
                                          return descriptor;  // Nếu không có id, nó sẽ bỏ qua
                                      })
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

                        channel.BasicConsume(queue: QUEUE_NAME, autoAck: false, consumer: consumer);

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