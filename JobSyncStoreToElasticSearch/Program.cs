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

        // Hàm gửi thông báo Telegram
        public static void SendMessageToTelegram(string token, string groupId, string message)
        {
            using (var client = new HttpClient())
            {
                var url = $"https://api.telegram.org/bot{token}/sendMessage";
                var data = new Dictionary<string, string>
        {
            { "chat_id", groupId },
            { "text", message }
        };

                client.PostAsync(url, new FormUrlEncodedContent(data)).Wait();
            }
        }


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
                                // Lấy dữ liệu từ message
                                var body = ea.Body.ToArray();
                                var message = Encoding.UTF8.GetString(body);

                                // Log message nhận được từ queue
                                Console.WriteLine($"[Log] Received message: {message}");

                                // Deserialize dữ liệu nhận được
                                DataInfoModel obj_data_queue = null;
                                try
                                {
                                    obj_data_queue = JsonConvert.DeserializeObject<DataInfoModel>(message);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[Error] Failed to deserialize message: {ex.Message}");
                                    SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Failed to deserialize message: {ex.Message}");
                                    // Gửi ACK để bỏ qua message không hợp lệ
                                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                    return; // Bỏ qua và xử lý message tiếp theo
                                }

                                // 1. Lấy kết quả từ database đổ về
                                var obj_config_info = StoreDataDAL.getDataFromStore(obj_data_queue);

                                if (obj_config_info == null || string.IsNullOrEmpty(obj_config_info.data_source))
                                {
                                    Console.WriteLine("[Warning] No data returned from database or data source is empty.");
                                    SendMessageToTelegram(tele_token, tele_group_id, "[Warning] No data returned from database or data source is empty.");
                                    // Gửi ACK để bỏ qua message không xử lý được
                                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                    return;
                                }

                                // Log dữ liệu nguồn
                                Console.WriteLine($"[Log] Data Source: {obj_config_info.data_source}");

                                // 2. Kết nối tới Elasticsearch
                                var nodes = new Uri[] { new Uri(obj_config_info.es_host_target) };
                                var connectionPool = new StaticConnectionPool(nodes);
                                var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex(obj_config_info.index_node);

                                var elasticClient = new ElasticClient(connectionSettings);

                                // Parse dữ liệu từ data_source
                                List<Dictionary<string, object>> dataList = null;
                                try
                                {
                                    dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(obj_config_info.data_source);
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[Error] Failed to deserialize data source: {ex.Message}");
                                    SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Failed to deserialize data source: {ex.Message}");
                                    // Gửi ACK để bỏ qua message lỗi
                                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                    return;
                                }

                                // Log danh sách dữ liệu
                                //Console.WriteLine($"[Log] Data List: {JsonConvert.SerializeObject(dataList)}");

                                // 3. Đẩy dữ liệu lên Elasticsearch
                                var bulkIndexResponse = elasticClient.Bulk(b => b
                                    .Index(obj_config_info.index_node)
                                    .IndexMany(dataList, (descriptor, item) =>
                                    {
                                        object idValue;
                                        if (item.TryGetValue("id", out idValue)) // Hoặc thay "id" bằng "OrderId"
                                        {
                                            return descriptor.Id(idValue.ToString());
                                        }
                                        return descriptor;
                                    })
                                );

                                // Kiểm tra kết quả trả về từ Elasticsearch
                                if (bulkIndexResponse.Errors)
                                {
                                    foreach (var item in bulkIndexResponse.ItemsWithErrors)
                                    {
                                        Console.WriteLine($"[Error] Failed to index document {item.Id}: {item.Error}");
                                    }
                                    SendMessageToTelegram(tele_token, tele_group_id, "[Error] Some documents failed to index.");
                                }
                                else
                                {
                                    Console.WriteLine("[Success] All documents indexed successfully.");
                                }

                                // Gửi ACK khi xử lý thành công
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Error] Exception while processing message: {ex.Message}");
                                SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Exception while processing message: {ex.Message}");

                                // Gửi ACK hoặc NACK để RabbitMQ không giữ message trong hàng đợi mãi
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
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