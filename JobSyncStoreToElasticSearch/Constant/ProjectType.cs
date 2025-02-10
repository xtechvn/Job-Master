namespace JobSyncStoreToElasticSearch.Constant
{
    public class ProjectType
    {
        public const int BIOLIFE = 0;
        public const int HULOTOYS = 1;
        public const int ADAVIGO_CMS = 2;
        public const int HOANBDS = 3;
        public const int DEEPSEEK_CMS = 5;

        public const int EDUCATION = 6;

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
                 case HOANBDS:
                    return "hoanbds_store";
                default:
                    return "unknown";
            }
        }
    }
}
