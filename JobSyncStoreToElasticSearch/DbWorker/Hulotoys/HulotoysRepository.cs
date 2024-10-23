using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobSyncStoreToElasticSearch.DbWorker.Hulotoys
{
    internal class HulotoysRepository:BaseDataRepository
    {
        public HulotoysRepository(string _connection, string _store_name) : base(_connection, _store_name) { }

      
    }
}
