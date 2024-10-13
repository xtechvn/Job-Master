namespace JobSyncStoreToElasticSearch.DbWorker.Biolife
{
    class BiolifeRepository : BaseDataRepository
    {

        public BiolifeRepository(string _connection, string _store_name) : base(_connection, _store_name) { }

        public virtual double getPageViewTotal()
        {
            return 0;
        }
    }
}
