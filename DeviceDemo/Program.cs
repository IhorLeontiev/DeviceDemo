using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.FileIO;
using Microsoft.Azure.Devices.Client;
using System.IO;
using System.Data;
using Newtonsoft.Json;

namespace DeviceDemo
{
    class Program
    {
        private const string DeviceConnectionString = "********************************************";
        static string data = string.Empty;
        static void Main(string[] args)
        {
            try
            {
                DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(DeviceConnectionString);
                if (deviceClient == null)
                {
                    Console.WriteLine("Failed to create DeviceClient!");
                }
                else
                {
                    SendEvent(deviceClient).Wait();
                    ReceiveCommands(deviceClient).Wait();
                }
                Console.WriteLine("Exited!\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
        }


        static async Task SendEvent(DeviceClient deviceClient)
        {
            string[] filePath = Directory.GetFiles(@"C:\Weblog\", "*.csv");
            string csv_file_path = string.Empty;
            int size = filePath.Length;
            for (int i = 0; i < size; i++)
            {
                Console.WriteLine(filePath[i]);
                csv_file_path = filePath[i];
            }
            DataTable csvData = GetDataTableFromCSVFile(csv_file_path);
            Console.WriteLine("Rows count:" + csvData.Rows.Count);
            DataTable table = csvData;
            foreach (DataRow row in table.Rows)
            {
                foreach (var item in row.ItemArray)
                    data = item.ToString();
                Console.Write(data);
                try
                {
                    foreach (DataRow rows in table.Rows)
                    {
                        var info = new WeatherData
                        {
                            weatherDate = rows.ItemArray[0].ToString(),
                            weatherTime = rows.ItemArray[1].ToString(),
                            Type = rows.ItemArray[2].ToString(),
                            AirTemperature = rows.ItemArray[3].ToString(),
                            RelativeHumidity = rows.ItemArray[4].ToString(),
                            WindSpeed = rows.ItemArray[5].ToString(),
                            SolarRadiation = rows.ItemArray[6].ToString(),
                            Temperature = rows.ItemArray[7].ToString(),
                           
                        };
                        var serializedString = JsonConvert.SerializeObject(info);
                        var message = data;
                        Console.WriteLine("{0}> Sending events: {1}", DateTime.Now.ToString(), serializedString.ToString());
                        await deviceClient.SendEventAsync(new Message(Encoding.UTF8.GetBytes(serializedString.ToString())));
                        
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{ 0} > Exception: { 1}", DateTime.Now.ToString(), ex.Message);
                    Console.ResetColor();
                }
            }
            Console.WriteLine("Press Ctrl - C to stop the sender process");
            Console.WriteLine("Press Enter to start now");
            Console.ReadLine();
        }
        private static DataTable GetDataTableFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            string data = string.Empty;
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    //read column names
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception" + ex.Message);
            }
            return csvData;
        }
        static async Task ReceiveCommands(DeviceClient deviceClient)
        {
            Console.WriteLine("\nDevice waiting for commands from IoTHub…\n");
            Message receivedMessage;
            string messageData;
            while (true)
            {
                receivedMessage = await deviceClient.ReceiveAsync(TimeSpan.FromSeconds(1));
                if (receivedMessage != null)
                {
                    messageData = Encoding.ASCII.GetString(receivedMessage.GetBytes());
                    Console.WriteLine("\t{ 0}> Received message: { 1}", DateTime.Now.ToLocalTime(), messageData);
                    await deviceClient.CompleteAsync(receivedMessage);
                }
            }
        }
    }
}





