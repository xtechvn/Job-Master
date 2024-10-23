namespace JobSyncStoreToElasticSearch.Constant
{
    public class ProjectType
    {
        public const int BIOLIFE = 0;
        public const int HULOTOYS = 1;
        public const int ADAVIGO_CMS = 2;
        public static string GetProjectTypeName(int projectType)
        {
            switch (projectType)
            {
                case BIOLIFE:
                    return "biolife_store";
                case HULOTOYS:
                    return "hulotoys_store";
                case ADAVIGO_CMS:
                    return "adavigo_store";
                default:
                    return "unknown";
            }
        }
    }
}
