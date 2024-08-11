
using System.Net;

namespace JobSyncStoreToElasticSearch.Common
{
    public class ErrorWriter
    {
        public static void WriteLog(string AppPath, string sFunction, string sAction)
        {
            StreamWriter sLogFile = null;
            try
            {
                //Ghi lại hành động của người sử dụng vào log file
                string sDay = string.Format("{0:dd}", DateTime.Now);
                string sMonth = string.Format("{0:MM}", DateTime.Now);
                string strLogFileName = sDay + "-" + sMonth + "-" + DateTime.Now.Year + ".log";
                string strFolderName = AppPath + @"\Logs\" + DateTime.Now.Year + "-" + sMonth;
                //Application.StartupPath
                //Tạo thư mục nếu chưa có
                if (!Directory.Exists(strFolderName + @"\"))
                {
                    Directory.CreateDirectory(strFolderName + @"\");
                }
                strLogFileName = strFolderName + @"\" + strLogFileName;

                if (File.Exists(strLogFileName))
                {
                    //Nếu đã tồn tại file thì tiếp tục ghi thêm
                    sLogFile = File.AppendText(strLogFileName);
                    sLogFile.WriteLine(string.Format("Thời điểm xảy ra lỗi: {0:hh:mm:ss tt}", DateTime.Now));
                    if (sFunction != string.Empty)
                        sLogFile.WriteLine(string.Format("Hàm/Phương thức sinh lỗi: {0}", sFunction));
                    sLogFile.WriteLine(string.Format("Chi tiết lỗi: {0}", sAction));
                    sLogFile.WriteLine("-------------------------------------------");
                    sLogFile.Flush();
                }
                else
                {
                    //Nếu file chưa tồn tại thì có thể tạo mới và ghi log
                    sLogFile = new StreamWriter(strLogFileName);
                    sLogFile.WriteLine(string.Format("Thời điểm xảy ra lỗi: {0:hh:mm:ss tt}", DateTime.Now));
                    if (sFunction != string.Empty)
                        sLogFile.WriteLine(string.Format("Hàm/Phương thức sinh lỗi: {0}", sFunction));
                    sLogFile.WriteLine(string.Format("Chi tiết lỗi: {0}", sAction));
                    sLogFile.WriteLine("-------------------------------------------");
                }
                sLogFile.Close();
            }
            catch (Exception)
            {
                if (sLogFile != null)
                {
                    sLogFile.Close();
                }
            }
        }

        public static void WriteLog(string AppPath, string sAction)
        {
            WriteLog(AppPath, string.Empty, sAction);
        }

        public static void WirteFile(string AppPath, string sContent)
        {
            StreamWriter sLogFile = null;
            try
            {
                sLogFile = new StreamWriter(AppPath);
                sLogFile.WriteLine(sContent);
                sLogFile.Close();
            }
            catch (Exception)
            {
                if (sLogFile != null)
                {
                    sLogFile.Close();
                }
            }
        }
        public static void InsertLogTelegramByUrl(string bot_token, string id_group, string msg)
        {
            string JsonContent = string.Empty;
            string url_api = "https://api.telegram.org/bot" + bot_token + "/sendMessage?chat_id=" + id_group + "&text=" + msg;
            try
            {
                using (var webclient = new WebClient())
                {
                    JsonContent = webclient.DownloadString(url_api);
                }
            }
            catch (Exception ex)
            {
                WriteLog("D://", ex.ToString());
            }
        }
    }
}
