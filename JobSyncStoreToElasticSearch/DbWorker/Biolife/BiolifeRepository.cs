namespace JobSyncStoreToElasticSearch.DbWorker.Biolife
{
    class BiolifeRepository: BaseDataRepository
    {
        public BiolifeRepository(string connection) 
            : base(connection) { }

        public virtual double getPageViewTotal()
        {
            return 0;
        }
    }
}
