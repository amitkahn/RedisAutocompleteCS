using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using ServiceStack.Text;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;


namespace RedisDAL
{
    public class AutoComplete
    {

        private static string apiUrl = "";
        private static string RedisHost = "server";
        private static int RedisPort = 6379;
        private static string userId = "15234206";
        private static int orgId = 19;
        private static int filter = 15;
        private static RedisClient redis;
        private static string redisKey;

        public Init(string RedisKey, bool clear, bool populate)
        {
            if (!String.IsNullOrEmpty(RedisKey))
                redisKey = RedisKey;

            if (clear)
                clearDictionary();

            if (populate)
                loadDictionary();
        }

        //Load the entries from dictionaryUrl. Add all the words along with all the possible prefixes of any word to Redis Set
        private void loadDictionary()
        {
            List<ACResult> results = GetAutoCompleteData();

            foreach (ACResult res in results)
            {
                addWord(res.Value.Trim());
            }
        }

        private void clearDictionary()
        {
            redis.Del(redisKey);
        }

        public List<String> complete(string prefix, int count)
        {
            if (String.IsNullOrEmpty(prefix))
                return null;

            int prefixLength = prefix.Length;
            ASCIIEncoding encoding = new ASCIIEncoding();
            Byte[] bytes = encoding.GetBytes(prefix);
            long start = redis.ZRank(redisKey, bytes);
            if (start < 0 || prefixLength == 0)
            {
                return null;    // Collections.emptyList();
            }
            List<String> results = new List<String>();
            List<string> rangeResults = new List<String>();
            int found = 0, rangeLength = 50, maxNeeded = count;

            while (found < maxNeeded)
            {
                byte[][] bytesArr = redis.ZRange(redisKey, (int)start, (int)start + rangeLength - 1);
                foreach (byte[] bres in bytesArr)
                {
                    rangeResults.Add(System.Text.Encoding.UTF8.GetString(bres));
                }

                start += rangeLength;
                if (rangeResults.Count == 0)
                {
                    break;
                }
                foreach (string entry in rangeResults)
                {
                    int minLength = Math.Min(entry.Length, prefixLength);
                    if (!entry.Substring(0, minLength).Equals(prefix.Substring(0, minLength), StringComparison.OrdinalIgnoreCase))
                    {
                        maxNeeded = results.Count;
                        break;
                    }
                    if (entry.EndsWith("*") && results.Count < maxNeeded)
                    {
                        results.Add(entry.Substring(0, entry.Length - 2));
                    }
                }
            }

            return results;
        }

        // Add all the possible prefixes of the given word and also the given word with a * suffix.
        private void addWord(string word)
        {
            ASCIIEncoding encoding = new ASCIIEncoding();
            Byte[] bytes = encoding.GetBytes(word + "*");
            redis.ZAdd(redisKey, 0, bytes);

            for (int index = 1, total = word.Length; index < total; index++)
            {
                Byte[] subWord = encoding.GetBytes(word.Substring(0, index));
                redis.ZAdd(redisKey, 0, subWord);
            }
        }


        private static List<ACResult> GetAutoCompleteData()
        {
            // http://server/api/en-US/ui/autocomplete/json?orgid=19&userid=15234206&options=15
            string url = String.Format(apiUrl + "userid={0}&orgid={1}&options={2}", userId, orgId.ToString(), filter.ToString());
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(new Uri(url));
            request.ContentLength = 0;
            request.Method = "GET";
            //request.ContentType = "application/x-www-form-urlencoded";

            //byte[] byteArray = null;
            Stream dataStream = null;
            /*
                if (request.Method.Equals("POST", StringComparison.InvariantCultureIgnoreCase))
                {
                    byteArray = Encoding.UTF8.GetBytes(postData);
                    request.ContentLength = byteArray.Length;
                    dataStream = request.GetRequestStream();
                    dataStream.Write(byteArray, 0, byteArray.Length);
                    dataStream.Close();
                }
           
              */
            WebResponse response = request.GetResponse();

            dataStream = response.GetResponseStream();

            //MemoryStream stream1 = new MemoryStream();            

            string responseFromServer;
            using (StreamReader reader = new StreamReader(dataStream))
            {
                responseFromServer = reader.ReadToEnd();
                responseFromServer = Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(responseFromServer));
            }

            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(ACResult));
            MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(responseFromServer));
            List<ACResult> results = serializer.ReadObject(ms) as List<ACResult>;
            ms.Close();

            return results;

        }

        /*
        public static void StoreAutoCompleteData()
        {
            string jsonResults = GetAutoCompleteData();

            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(List<ACResult>));
            MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(jsonResults));
            List<ACResult> results = serializer.ReadObject(ms) as List<ACResult>;
            ms.Close();

            redis = new RedisClient(RedisHost, RedisPort);
            if (!redis.Ping())
            {
                Console.WriteLine("Redis Connection error");
                return;
            }

            redis.RemoveAll(redis.GetAllKeys());

            using (var redisResults = redis.As<ACResult>())
            {
                //redisResults.Store(GetAutoCompleteData(userId, orgId, filter));
                //redisResults.StoreAll(results);

                ACResult acr = new ACResult { id = redisResults.GetNextSequence(), Value="Amit test", Lat=32.23232, Long=22.454545, Type=3};
                redisResults.Store(acr);
                var res = redisResults.GetAll();
                Console.WriteLine(res.Dump());
                
                //redisResults.Store(new ACResult { Id = redisResults.GetNextSequence(), Value = "acresult" });
                //var allUsers = redisResults.GetAll();

                //Recursively print the values of the POCO
                // allUsers.Clear();
                //Assert.That(redis.GetValue(redis.NamespacePrefix + "urn:mypoco:1"), Is.EqualTo("{\"Id\":1,\"Name\":\"Test\"}"));
            }

        }
*/
    }
    //class ACResultCollection
    //{
    //    public IEnumerable<ACResult> acResults { get; set; }
    //}

    [DataContract]
    class ACResult
    {
        [DataMember]
        internal long id { get; set; }
        [DataMember]
        internal string Value { get; set; }
        [DataMember]
        internal double? Lat { get; set; }
        [DataMember]
        internal double? Long { get; set; }
        [DataMember]
        internal int? Type { get; set; }
        [DataMember]
        internal int? LastUpdateHours { get; set; }
    }

//id: 1,
//Value: "גולן",
//Lat: 35.670829,
//Long: 33.043396,
//Type: 8,
//LastUpdateHours: null

}
