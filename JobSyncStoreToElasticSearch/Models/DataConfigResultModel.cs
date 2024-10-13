namespace JobSyncStoreToElasticSearch.Models
{
    public class DataConfigResultModel
    {
        public string data_source { get; set; }
        public string index_node { get; set; }
        public string es_host_target { get; set; }
    }
}
