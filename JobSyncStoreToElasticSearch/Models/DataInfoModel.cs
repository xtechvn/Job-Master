namespace JobSyncStoreToElasticSearch.Models
{
    public class DataInfoModel
    {
        public string store_name { get; set; } // Tên store cần sync
        public string index_es { get; set; } // tên index ES. đặt tên chữ viết thường phân cách bởi _

        public string project_type { get; set; } // tên catalog
    }
}
