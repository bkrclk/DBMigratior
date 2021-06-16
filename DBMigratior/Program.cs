using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace DBMigratior
{
    class Program
    {
        static string mysqlLocal = "server=localhost;port=13038;database=ledms;user=root;password=root;Max Pool Size=200;Min Pool Size=10;Pooling=true";
        static string mysqlTest = "server=192.168.2.58;port=30051;database=ledms;user=xxxxx;password=xxxx;Max Pool Size=200;Min Pool Size=10;Pooling=true";
        static void Main(string[] args)
        {

            Connet();
            Console.WriteLine("Bağlandı!");
        }

        static void Connet()
        {
            using var localDbconnection = new MySqlConnection(mysqlLocal);
            localDbconnection.Open();

            using var testDbconnection = new MySqlConnection(mysqlTest);
            testDbconnection.Open();

            List<string> tableNameList = new List<string>();

            string getAllTablesName = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'ledms'";
            var command = new MySqlCommand(getAllTablesName, localDbconnection);

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var value = reader.GetValue(0);
                    tableNameList.Add(value.ToString());
                }
            }

            localDbconnection.Close();

            foreach (var i in tableNameList)
            {
                var tableCountTest = $"SELECT * FROM {i} LIMIT 10";
                using var command2 = new MySqlCommand(tableCountTest, testDbconnection);
                using var reader2 = command2.ExecuteReader();

                while (reader2.Read())
                {
                    try
                    {
                        Dictionary<string, object> tableRowData = new Dictionary<string, object>();
                        //List<object> tableRowValue = new List<object>();
                        var strColumValue = "";
                        var strColumName = "";

                        for (int t = 0; t < reader2.FieldCount; t++)
                        {

                            tableRowData.Add(reader2.GetName(t), reader2.GetValue(t));
                            if (!string.IsNullOrEmpty(reader2.GetValue(t).ToString()))
                            {
                                string strValue = reader2.GetValue(t).ToString();

                                Double decValue = 0.0;

                                if (DateTime.TryParse(strValue, CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime time))
                                {
                                    strColumValue += "STR_TO_DATE('" + reader2.GetValue(t) + "','%d.%m.%Y %H:%i:%s')";
                                }
                                else if (Double.TryParse(strValue, out decValue))
                                {
                                    var doubleValue = reader2.GetValue(t);

                                    strColumValue += ("'" + doubleValue + "'").Replace(',', '.');
                                }
                                else
                                {
                                    strColumValue += "'" + reader2.GetValue(t) + "'";
                                }

                                strColumName += reader2.GetName(t);
                                strColumValue += ",";
                                strColumName += ",";
                            }
                        }
                        strColumValue = strColumValue.Substring(0, strColumValue.Length - 1);
                        strColumName = strColumName.Substring(0, strColumName.Length - 1);

                        localDbconnection.Close();

                        var commandStr = $"insert into {i} ({strColumName}) values ({strColumValue}) ";

                        using (var command3 = new MySqlCommand(commandStr, localDbconnection))
                        {
                            localDbconnection.Open();
                            command3.ExecuteNonQuery();
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                }
            }
        }
    }
}
