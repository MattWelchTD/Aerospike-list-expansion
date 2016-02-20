using System;
using Aerospike.Client;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;


namespace aerospikelistexpansion
{
	class MainClass
	{
		public const string asServerIP = "172.28.128.6";
		public const int asServerPort = 3000;
		public const string ns = "test";
		public const string seqSet = "events";
		public const string cdtSet = "cdtList";
		public const string ldtSet = "ldtList";
		public const string listBinName = "event-list";
		public const string seqBinName = "seq";
		public const string eventBinName = "event";
		public const string keyBinName = "key";
		public const string accBinName = "acc";
		public const string accDayBinName = "accDay";
		public const string ldtBinName = "prodListLDT";
		public const string productPositionBinName = "prodPosition";
		public const string prodNameBinName = "prodName";
		public const string cdtBinName = "prodListCDT";
		public const int accountTotal = 500;
		public const int productTotal = 50000;
		public const string LDT_KEY = "key";
		public const string LDT_VALUE = "value";
		public const int days = 50;

		public static void Main (string[] args)
		{
			AerospikeClient client = null;
			try {
				Console.WriteLine ("INFO: Connecting to Aerospike cluster...");
				Stopwatch stopwatch = new Stopwatch();
				//Thread[] array = new Thread[accountTotal];
			


				// Establish connection
				client = new AerospikeClient (asServerIP, asServerPort);

				// Check to see if the cluster connection succeeded
				if (client.Connected) {
					Console.WriteLine("INFO: Connection to Aerospike cluster succeeded!\n");
					int feature = 0;
					while (feature != 99) {
					// Present options
					Console.WriteLine("\n\nWhat would you like to do:");
					Console.WriteLine("1> Generate data files for last "+days+" days");
					Console.WriteLine("2> Timeseries using sequence");
					Console.WriteLine("3> Generate Position using CDT and LDT");
					Console.WriteLine("4> List CDT vs LDT - whole list");
					Console.WriteLine("5> List CDT vs LDT - one element");
					Console.WriteLine("99> Exit");
					Console.Write("\nSelect 0-4 and hit enter:");
					feature = int.Parse(Console.ReadLine());
					if (feature != 99)
					{
						switch (feature)
						{
						case 1:
							Console.WriteLine("Generating data...");
							DateTime today = DateTime.Now;
							today = today.Date;

							// write yesterday to Aerospike
							DateTime yesterday = today.AddDays(-1);
							generateTimeSeries(yesterday.ToShortDateString(), client);

							// write yesterday to file
							generateTimeSeries(yesterday.ToShortDateString(), null);
								

							// write n days
							for (int dif = 2; dif < days; dif ++){
								DateTime dayBefore = today.AddDays(-dif);
								Console.WriteLine("Writing: " + dayBefore.ToShortDateString());
								generateTimeSeries(dayBefore.ToShortDateString(), client);
							}
							Console.WriteLine("Generating data completed");
							break;
						case 3:
							Console.WriteLine("Generating position data using CDT and LDT...");
							generateCustomerProduct(client);
							Console.WriteLine("Generating data completed");
							break;
						case 4:
							
							Console.WriteLine("CDT vs LDT - Whole list...");
							int cdtSize = 0;
							stopwatch.Start();
							for (int acc = 0; acc < accountTotal; acc++){
								string accString = (acc+1).ToString();
								//array[acc] = new Thread(() => getAllProductsCDT(client, accString));
								//array[acc].Start();
									cdtSize += getAllProductsCDT(client, accString).Count;
							}
							//for (int i = 0; i < accountTotal; i++)
							//{
							//	array[i].Join();
							//}
							
							stopwatch.Stop();
							Console.WriteLine("CDT latency: " + stopwatch.ElapsedMilliseconds/accountTotal + "ms for avg list size " + cdtSize/accountTotal);

							stopwatch.Reset();
							int ldtSize = 0;
							stopwatch.Start();
							for (int acc = 0; acc < accountTotal; acc++){
								
								string accString = (acc+1).ToString();
								//array[acc] = new Thread(() => getAllProductsLDT(client, accString));
								//array[acc].Start();
								IList res = getAllProductsLDT(client, accString);
									if (res != null)
										ldtSize += res.Count;
							}
							//for (int i = 0; i < accountTotal; i++)
							//{
							//	array[i].Join();
							//}
							stopwatch.Stop();
							Console.WriteLine("LDT latency: " + stopwatch.ElapsedMilliseconds/accountTotal + "ms for avg list size " + ldtSize/accountTotal);

							break;
						case 5:
								const int attempts = 50000; 
								Console.WriteLine("CDT vs LDT - one element..., {0} attempts", attempts);
								long cdtTotal = 0, ldtTotal = 0, prodCount = 0;
								Random accRand = new Random(12121);
								for (int i = 0; i < 50000; i++){
										string accString = accRand.Next(1, accountTotal).ToString();
										Dictionary<string, int> prods = getAllProductsCDT(client, accString);
										prodCount += prods.Count;
										foreach (KeyValuePair<string, int> pair in prods){
											// CDT
											stopwatch.Start();
											listCDTFind(client, accString, pair.Key);
											stopwatch.Stop();
											cdtTotal += stopwatch.ElapsedMilliseconds;

											stopwatch.Reset();

											// LDT
											stopwatch.Start();
											listCDTFind(client, accString, pair.Key);
											stopwatch.Stop();
											ldtTotal += stopwatch.ElapsedMilliseconds;

										}
								}
								Console.WriteLine("CDT avg latency: {0:F5} ms", (double)cdtTotal/prodCount);
								Console.WriteLine("LDT avg latency: {0:F5} ms", (double)ldtTotal/prodCount);

							break;
						default:
							
							break;
						}
					}
						}
				}
			} catch (AerospikeException e) {
				Console.WriteLine ("AerospikeException - Message: " + e.Message);
				Console.WriteLine ("AerospikeException - StackTrace: " + e.StackTrace);
			} catch (Exception e) {
				Console.WriteLine ("Exception - Message: " + e.Message);
				Console.WriteLine ("Exception - StackTrace: " + e.StackTrace);
			} finally {
				if (client != null && client.Connected) {
					// Close Aerospike server connection
					client.Close ();
				}

			}
		}

		public static void cats(){
		}

		public static void generateCustomerProduct(AerospikeClient client){
			Random products = new Random(2727);
			Random productsPerAccount = new Random(9898);
			Random productQuantity = new Random (1919);
			for (int i = 0; i < accountTotal; i++) {
				
				int productsToAdd = productsPerAccount.Next (1, 150);
				string keyString = i.ToString();
				Key cdtkey = new Key (ns, cdtSet, keyString);
				Key ldtkey = new Key (ns, ldtSet, keyString);
				client.Put (null, cdtkey, new Bin(keyBinName, keyString), new Bin(accBinName, keyString));
				client.Put (null, ldtkey, new Bin(keyBinName, keyString), new Bin(accBinName, keyString));
				for (int j = 0; j < productsToAdd; j++) {
					int product = products.Next (1, productTotal);
					string productString = product.ToString ();
					int productAmount = productQuantity.Next (1, 100);

					listCDTAdd(client, keyString, productString, productAmount);

					listLDTAdd (client, keyString, productString, productAmount);

				}
			}
		}

		public static Dictionary<string, int> getAllProductsCDT(AerospikeClient client, string account){
			Key cdtkey = new Key (ns, cdtSet, account);
			Dictionary<string, int> products = new Dictionary<string, int> ();
			Record record = client.Get (null, cdtkey, cdtBinName);
			if (record != null) {
				IList receivedList = record.GetList (cdtBinName);
				List<Key> subKeys = new List<Key> ();
				foreach (object digest in receivedList) {
					subKeys.Add (new Key (ns, (byte[])digest, null, null));
				}
				Record[] productRecords = client.Get (null, subKeys.ToArray ());
				foreach (Record productRecord in productRecords) {
					products [productRecord.GetString (prodNameBinName)] = 
					productRecord.GetInt (productPositionBinName);
				}
			}
			return products;
		}

		public static IList getAllProductsLDT(AerospikeClient client, string account){
			Key ldtkey = new Key (ns, ldtSet, account);
			LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);
			IList result = llist.Scan();
			return result;
		}

		public static Dictionary<string, int> listCDTFind(AerospikeClient client, string account, string product){
			String subKeyString = account + ":" + product;
			Key subKey = new Key (ns, cdtSet, subKeyString);
			Dictionary<string, int> foundProduct = new Dictionary<string, int>();
			Record record = client.Get(null, subKey, productPositionBinName, accBinName, prodNameBinName);
			if (record != null){
				foundProduct [record.GetString (prodNameBinName)] = record.GetInt (productPositionBinName);
			}
			return foundProduct;
		}

		public static void listCDTAdd(AerospikeClient client, string account, string product, int productAmount){
			Key cdtkey = new Key (ns, cdtSet, account);
			String subKeyString = account + ":" + product;
			Key subKey = new Key (ns, cdtSet, subKeyString);

			client.Operate(null, subKey, 
				Operation.Add(new Bin(productPositionBinName, productAmount)), 
				Operation.Put(new Bin(keyBinName, subKeyString)),
				Operation.Put(new Bin(accBinName, account)),
				Operation.Put(new Bin(prodNameBinName, product))
			);
			/*
			 * add the digest of the subKey to the CDT List in the Customer record
			 */
			client.Operate(null, cdtkey, ListOperation.Append(cdtBinName, Value.Get(subKey.digest)));

		}

		public static IList listLDTFind(AerospikeClient client, string account, string product){
			Key ldtkey = new Key (ns, ldtSet, account);
			LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);
			Dictionary<string, Object> keyMap = makeKeyMap (product);

			IList foundProd = llist.Find (Value.Get(keyMap));
			return foundProd;
		}

		public static void listLDTAdd(AerospikeClient client, string account, string product, int productAmount){
			Key ldtkey = new Key (ns, ldtSet, account);
			LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);
			Value value = makeValue (product, productAmount);

			llist.Update (value);
		}

		public static void generateTimeSeries(string date, AerospikeClient client = null){
			string[] txTypes = {
				"buy",
				"bid",
				"sell",
				"cancelled"
			};
			string dateString = date.Replace ('/', '-');
			string fileName = "data/" + dateString + ".csv";
			int records = (client != null) ? 10000 : 9800;
			Random accounts = new Random(9898);
			Random products = new Random(2727);
			Random txTypeNext = new Random (1919);
			using (System.IO.StreamWriter file = 
				new System.IO.StreamWriter ( fileName ) )
				{
					for (int i = 0; i < records; i++) {
						int account = accounts.Next (1, accountTotal); 
						
						int product = products.Next (1, productTotal);
						string txType = txTypes [txTypeNext.Next (0, txTypes.Length)];
						string eventString = product + "," + txType;

						if (client != null) {
							/*
							 * write to aerospike using seq
							 */
							int seq = 1;
							int eventSize = 0;

							string accDayString = account + "::" + date;
							string keyString = accDayString + "::" + seq;
							Key key = new Key (ns, seqSet, keyString);
							/*
							 * get the size of the event list in first record
							 */
							Record record = client.Operate (null, key, ListOperation.Size (listBinName), Operation.Get (seqBinName));
							
							if (record != null) {
								eventSize = record.GetInt (listBinName);
								
								if (eventSize == 30) {
									record = client.Operate (null, key, Operation.Add (new Bin (seqBinName, 1)), Operation.Get (seqBinName));
									seq = record.GetInt (seqBinName);
									keyString = account + "::" + date + "::" + seq;
									key = new Key (ns, seqSet, keyString);
								}
							} 

						client.Operate (null, key, Operation.Put(new Bin(accDayBinName, accDayString)), 
							Operation.Put(new Bin(keyBinName, keyString)), 
							Operation.Put(new Bin(seqBinName, seq)), 
							ListOperation.Append (listBinName, Value.Get (eventString)));
						} 
						/*
						 * write to file
						 */
						file.WriteLine (account + "," + eventString);
					

					}
				//file.close ();
			}
		}


		public static Dictionary<string, Object> makeKeyMap(Object key){
			Dictionary<string, Object> map = new Dictionary<string, Object>();
			map[LDT_KEY] = key;
			return map;
		}

		public static Dictionary<string, Object> makeValueMap(Object key, Object value){
			Dictionary<string, Object> map = new Dictionary<string, Object>();
			map[LDT_KEY] = key;
			map[LDT_VALUE] = value;
			return map;
		}
		public static Value makeValue(Object key, Object value){
			return Value.Get(makeValueMap(key, value));
		}

	}

}
