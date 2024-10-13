namespace JobSyncStoreToElasticSearch.Models
{
    public class DataInfoModel
    {
        public string store_name { get; set; } // Tên store cần sync
        public string index_es { get; set; } // tên index ES. đặt tên chữ viết thường phân cách bởi
        public int project_type { get; set; } // type của dự án. Dựa vào đây để detect điều hướng sang các connection
        public string id { get; set; } // Khóa chỉnh của bản ghi       
       
    }
}
