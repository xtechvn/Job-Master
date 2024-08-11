using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Nest;

namespace JobSyncStoreToElasticSearch.ElasticSearch
{
    //https://www.steps2code.com/post/how-to-use-elasticsearch-in-csharp
    public class ESRepository<TEntity> : IESRepository<TEntity> where TEntity : class
    {
        private readonly IConfiguration configuration;
        private static string _ElasticHost;

        public ESRepository(string Host, IConfiguration _configuration)
        {
            _ElasticHost = Host;
            this.configuration = _configuration;
        }



        public int UpSert(TEntity entity, string indexName)
        {
            try
            {
                var nodes = new Uri[] { new Uri(_ElasticHost) };
                var connectionPool = new StaticConnectionPool(nodes);
                var connectionSettings = new ConnectionSettings(connectionPool).DisableDirectStreaming().DefaultIndex("people");
                var elasticClient = new ElasticClient(connectionSettings);
                var indexResponse = elasticClient.Index(new IndexRequest<TEntity>(entity, indexName));

                if (!indexResponse.IsValid)
                {
                   // LogHelper.WriteLogActivity(Directory.GetCurrentDirectory(), indexResponse.OriginalException.Message);
                   // LogHelper.WriteLogActivity(Directory.GetCurrentDirectory(), "apicall" + indexResponse.ApiCall.OriginalException.Message);
                    return 0;
                }

                return 1;
            }
            catch (Exception ex)
            {
               // LogHelper.InsertLogTelegramByUrl(configuration["telegram:log_try_catch:bot_token"], configuration["telegram:log_try_catch:group_id"], MethodBase.GetCurrentMethod().Name + "=>" + ex.ToString());
                return -1;
            }
        }




    }
}
