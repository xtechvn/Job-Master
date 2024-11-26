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
        public static string es_host_target = ConfigurationManager.AppSettings["es_master"];

        public static void SendMessageToTelegram(string token, string groupId, string message)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    var url = $"https://api.telegram.org/bot{token}/sendMessage";
                    var data = new Dictionary<string, string>
                    {
                        { "chat_id", groupId },
                        { "text", message }
                    };

                    Console.WriteLine($"[Log] Sending message to Telegram: {message}");
                    var response = client.PostAsync(url, new FormUrlEncodedContent(data)).Result;

                   
                   
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Error] Exception in SendMessageToTelegram: {ex.Message}");
                Console.WriteLine($"[Error] StackTrace: {ex.StackTrace}");
            }
        }

        static void Main(string[] args)
        {
            try
            {
                var factory = new ConnectionFactory()
                {
                    HostName = QUEUE_HOST,
                    UserName = QUEUE_USERNAME,
                    Password = QUEUE_PASSWORD,
                    VirtualHost = QUEUE_V_HOST,
                    Port = int.Parse(QUEUE_PORT)
                };

                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
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
                            Console.WriteLine("-----------------------------------------------------------------------------");
                            Console.WriteLine($"[TIME] {DateTime.Now}");
                            Console.WriteLine($"[Log] Received message: {message}");
                            SendMessageToTelegram(tele_token, tele_group_id, $"[Log] Received data: {message}");

                            DataInfoModel obj_data_queue = null;
                            try
                            {
                                obj_data_queue = JsonConvert.DeserializeObject<DataInfoModel>(message);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("-----------------------------------------------------------------------------");
                                Console.WriteLine($"[TIME] {DateTime.Now}");
                                Console.WriteLine($"[Error] Failed to deserialize message: {ex.Message}");
                                SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Failed to deserialize message: {ex.Message}");
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                return;
                            }

                            var obj_config_info = StoreDataDAL.getDataFromStore(obj_data_queue);
                            if (obj_config_info == null || string.IsNullOrEmpty(obj_config_info.data_source))
                            {
                                Console.WriteLine("[Warning] No data returned from database or data source is empty.");
                                SendMessageToTelegram(tele_token, tele_group_id, "[Warning] No data returned from database or data source is empty.");
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                return;
                            }

                            //Console.WriteLine($"[Log] Data Source: {obj_config_info.data_source}");

                            var nodes = new Uri[] { new Uri(obj_config_info.es_host_target) };
                            var connectionPool = new StaticConnectionPool(nodes);
                            var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex(obj_config_info.index_node);

                            var elasticClient = new ElasticClient(connectionSettings);

                            List<Dictionary<string, object>> dataList = null;
                            try
                            {
                                dataList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(obj_config_info.data_source);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Error] Failed to deserialize data source: {ex.Message}");
                                SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Failed to deserialize data source: {ex.Message}");
                                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                                return;
                            }

                            //Console.WriteLine($"[Log] Data List: {JsonConvert.SerializeObject(dataList)}");

                            var bulkIndexResponse = elasticClient.Bulk(b => b
                                .Index(obj_config_info.index_node)
                                .IndexMany(dataList, (descriptor, item) =>
                                {
                                    object idValue;
                                    if (item.TryGetValue("id", out idValue))
                                    {
                                        return descriptor.Id(idValue.ToString());
                                    }
                                    return descriptor;
                                })
                            );

                            if (!bulkIndexResponse.Errors)
                            {
                                Console.WriteLine("[Success] All documents indexed successfully.");
                                SendMessageToTelegram(tele_token, tele_group_id, "[Success] All documents indexed successfully.");
                            }
                            else
                            {
                                Console.WriteLine("[Error] Bulk index operation failed.");
                                foreach (var item in bulkIndexResponse.ItemsWithErrors)
                                {
                                    Console.WriteLine($"[Error] Failed to index document {item.Id}: {item.Error}");
                                }
                                SendMessageToTelegram(tele_token, tele_group_id, "[Error] Bulk index operation failed.");
                            }

                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Error] Exception while processing message: {ex.Message}");
                            SendMessageToTelegram(tele_token, tele_group_id, $"[Error] Exception while processing message: {ex.Message}");
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                    };

                    channel.BasicConsume(queue: QUEUE_NAME, autoAck: false, consumer: consumer);
                    Console.ReadLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Critical Error] Main function exception: {ex.Message}");
                SendMessageToTelegram(tele_token, tele_group_id, $"[Critical Error] Main function exception: {ex.Message}");
            }
        }
    }
}
