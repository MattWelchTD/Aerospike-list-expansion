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
		public const string dayBinName = "day";
		public const string ldtBinName = "prodListLDT";
		public const string productPositionBinName = "prodPosition";
		public const string prodNameBinName = "prodName";
		public const string cdtBinName = "prodListCDT";
		public const int accountTotal = 500;
		public const int productTotal = 50000;
		public const string LDT_KEY = "key";
		public const string LDT_VALUE = "value";
		public const int days = 50;
		public const string udfDir = "/Users/peter/git/aerospike/Aerospike-list-expansion/aerospike-list-expansion/";

		public static void Main (string[] args)
		{
			AerospikeClient client = null;
			try {
				Console.WriteLine ("Connecting to Aerospike cluster...");
				Stopwatch stopwatch = new Stopwatch ();
				//Thread[] array = new Thread[accountTotal];
			


				// Establish connection
				client = new AerospikeClient (asServerIP, asServerPort);

				// Check to see if the cluster connection succeeded
				if (client.Connected) {
					Console.WriteLine ("Connection succeeded!\n");
					int feature = 0;
					while (feature != 99) {
						// Present options
						Console.WriteLine ("\n\nWhat would you like to do:");
						Console.WriteLine ("1> Generate sequence data for last " + days + " days");
						Console.WriteLine ("2> Reconsile sequence data");
						Console.WriteLine ("3> Generate Position using CDT and LDT");
						Console.WriteLine ("4> List CDT vs LDT - whole list");
						Console.WriteLine ("5> List CDT vs LDT - one element");
						Console.WriteLine ("99> Exit");
						Console.Write ("\nSelect 1-5, 99 and hit enter:");
						string input = Console.ReadLine ();
						if (input.Trim().Length == 0)
							feature = 99;
						else
							feature = int.Parse (input);
						if (feature != 99) {
							switch (feature) {
							case 1:
								Console.WriteLine ("Generating data...");
								DateTime today = DateTime.Now;
								today = today.Date;

							// write yesterday to Aerospike
								DateTime yesterday = today.AddDays (-1);
								generateTimeSeries (yesterday.ToShortDateString (), client);

							// write yesterday to file
								generateTimeSeries (yesterday.ToShortDateString (), null);
								

							// write n days
								for (int dif = 2; dif < days; dif++) {
									DateTime dayBefore = today.AddDays (-dif);
									Console.WriteLine ("Writing: " + dayBefore.ToShortDateString ());
									generateTimeSeries (dayBefore.ToShortDateString (), client);
								}
								Console.WriteLine ("Generating data completed");
								break;
							case 2:
								Console.WriteLine ("Reconsiling yesterday's data");

								#region setup
								// use admin tools to do this in production
								RegisterTask rTask = client.Register(null, 
									udfDir + "utility.lua", 
									"utility.lua", Language.LUA);
								while(!rTask.QueryIfDone()){
									Thread.Sleep(20);
								}

								client.CreateIndex(null, ns, seqSet, 
													"day-seq-index", dayBinName, IndexType.STRING);
								#endregion
								today = DateTime.Now;
								today = today.Date;
								yesterday = today.AddDays (-1);
								string yesterdayString = yesterday.ToShortDateString ();
								// purge first sequence
								PurgeDay (yesterdayString, client);
								/// reconciliation from file
								ReconcileDay (yesterdayString, client);

								Console.WriteLine ("Reconciliation complete");
								break;
							case 3:
								Console.WriteLine ("Generating position data using CDT and LDT...");
								generateCustomerProduct (client);
								Console.WriteLine ("Generating data completed");
								break;
							case 4:
							
								Console.WriteLine ("CDT vs LDT - Whole list...");
								long cdtTotal = 0, ldtTotal = 0, cdtProdCount = 0, ldtProdCount = 0;

								for (int acc = 0; acc < accountTotal; acc++) {
									string accString = (acc + 1).ToString ();
									Key cdtkey = new Key (ns, cdtSet, accString);
									Key ldtkey = new Key (ns, ldtSet, accString);
									Aerospike.Helper.Collection.LargeList clist = new Aerospike.Helper.Collection.LargeList (client, null, cdtkey, cdtBinName);
									LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);
									stopwatch.Start ();
									IList cresult = clist.Scan ();
									stopwatch.Stop ();
									if (cresult != null)
										cdtProdCount += cresult.Count;
									cdtTotal += stopwatch.ElapsedMilliseconds;
									stopwatch.Reset ();

									stopwatch.Start ();
									IList lresult = llist.Scan ();
									stopwatch.Stop ();
									if (lresult != null)
										ldtProdCount += lresult.Count;
									ldtTotal += stopwatch.ElapsedMilliseconds;
								}

								Console.WriteLine ("CDT avg latency: {0:F5} ms", (double)cdtTotal / cdtProdCount);
								Console.WriteLine ("LDT avg latency: {0:F5} ms", (double)ldtTotal / ldtProdCount);


								break;
							case 5:
								const int attempts = 50000; 
								Console.WriteLine ("CDT vs LDT - one element..., {0} customers, each product for a customer", attempts);
								cdtTotal = 0;
								ldtTotal = 0; 
								long prodCount = 0;
								Random accRand = new Random (12121);
								for (int i = 0; i < 50000; i++) {
									string accString = accRand.Next (1, accountTotal).ToString ();
									Key cdtkey = new Key (ns, cdtSet, accString);
									Key ldtkey = new Key (ns, ldtSet, accString);
									Aerospike.Helper.Collection.LargeList clist = new Aerospike.Helper.Collection.LargeList (client, null, cdtkey, cdtBinName);
									LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);
									IList prods = llist.Scan ();
									if (prods != null) {
										prodCount += prods.Count;
										foreach (IDictionary product in prods) {
											//Dictionary<string, Object> keyMap = makeKeyMap (product);
											// CDT
											stopwatch.Start ();

											clist.Find (Value.Get (product));

											stopwatch.Stop ();
											cdtTotal += stopwatch.ElapsedMilliseconds;

											stopwatch.Reset ();

											// LDT
											stopwatch.Start ();

											llist.Find (Value.Get (product));

											stopwatch.Stop ();
											ldtTotal += stopwatch.ElapsedMilliseconds;

										}
									}
								}
								Console.WriteLine ("CDT avg latency: {0:F5} ms", (double)cdtTotal / prodCount);
								Console.WriteLine ("LDT avg latency: {0:F5} ms", (double)ldtTotal / prodCount);

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

		#region LDT-replacement
		public static void generateCustomerProduct (AerospikeClient client)
		{
			Random products = new Random (2727);
			Random productsPerAccount = new Random (9898);
			Random productQuantity = new Random (1919);
			for (int i = 0; i < accountTotal; i++) {
				
				int productsToAdd = productsPerAccount.Next (1, 150);
				string keyString = i.ToString ();
				Key cdtkey = new Key (ns, cdtSet, keyString);
				Aerospike.Helper.Collection.LargeList clist = new Aerospike.Helper.Collection.LargeList (client, null, cdtkey, cdtBinName);
				Key ldtkey = new Key (ns, ldtSet, keyString);
				LargeList llist = client.GetLargeList (null, ldtkey, ldtBinName);

				//for diagnositics
				client.Put (null, cdtkey, new Bin (keyBinName, keyString), new Bin (accBinName, keyString));
				client.Put (null, ldtkey, new Bin (keyBinName, keyString), new Bin (accBinName, keyString));

				for (int j = 0; j < productsToAdd; j++) {
					int product = products.Next (1, productTotal);
					int productAmount = productQuantity.Next (1, 100);
					Value value = makeValue (product, productAmount);

					llist.Update (value);
					clist.Update (value);

				}
			}
		}
		#endregion


		#region Time-series
		public static void PurgeDay (string date, AerospikeClient client)

		{
			Console.WriteLine ("Purgin data for {0}", date);
			Stopwatch stopwatch = new Stopwatch ();
			stopwatch.Start ();
			Statement statement = new Statement ();
			statement.Namespace = ns;
			statement.SetName = seqSet;

			//statement.SetFilters (Filter.Equal (dayBinName, date));

			ExecuteTask task = client.Execute (null, statement, "utility", "purge", 
				Value.Get(dayBinName),
				Value.Get(date));

			task.Wait ();
			stopwatch.Stop ();
			Console.WriteLine ("Purge completed {0} ms", stopwatch.ElapsedMilliseconds);
		}

		public static long ReconcileDay (string date, AerospikeClient client)
		{
			Console.WriteLine ("Reconsiling data for {0}", date);
			long counter = 0;
			Stopwatch stopwatch = new Stopwatch ();
			stopwatch.Start ();
			string dateString = date.Replace ('/', '-');
			string fileName = "data/" + dateString + ".csv";
			string line;
			System.IO.StreamReader file = 
				new System.IO.StreamReader (fileName);
			while ((line = file.ReadLine ()) != null) {
				// part[0] account
				// part[1] product
				// part[2] tx type
				string[] parts = line.Split (',');
				WriteEventToAerospike(date, client, parts[0], parts[1], parts[2]);
				//Console.WriteLine (line);
				counter++;
			}

			file.Close ();
			stopwatch.Stop ();
			Console.WriteLine ("Reconsiliation completed {0} ms", stopwatch.ElapsedMilliseconds);
			return counter;
		}

		public static void generateTimeSeries (string date)
		{
			generateTimeSeries (date, null);
		}

		public static void generateTimeSeries (string date, AerospikeClient client = null)
		{
			string[] txTypes = {
				"buy",
				"bid",
				"sell",
				"cancelled"
			};
			string dateString = date.Replace ('/', '-');
			string fileName = "data/" + dateString + ".csv";
			int records = (client != null) ? 10000 : 9800;
			Random accounts = new Random (9898);
			Random products = new Random (2727);
			Random txTypeNext = new Random (1919);
			using (System.IO.StreamWriter file = 
				       new System.IO.StreamWriter (fileName)) {
				for (int i = 0; i < records; i++) {
					int account = accounts.Next (1, accountTotal); 
						
					int product = products.Next (1, productTotal);
					string txType = txTypes [txTypeNext.Next (0, txTypes.Length)];
					string eventString = product + "," + txType;

					if (client != null) {

						WriteEventToAerospike (date, client, account.ToString (), product.ToString (), txType);
						/*
						 * write to aerospike using seq
						 */
//						int seq = 1;
//						int eventSize = 0;
//
//						string accDayString = account + "::" + date;
//						string keyString = accDayString + "::" + seq;
//						Key key = new Key (ns, seqSet, keyString);
//						/*
//						 * get the size of the event list in first record
//						 */
//						Record record = client.Operate (null, key, ListOperation.Size (listBinName), Operation.Get (seqBinName));
//							
//						if (record != null) {
//							eventSize = record.GetInt (listBinName);
//								
//							if (eventSize == 30) {
//								record = client.Operate (null, key, Operation.Add (new Bin (seqBinName, 1)), Operation.Get (seqBinName));
//								seq = record.GetInt (seqBinName);
//								keyString = account + "::" + date + "::" + seq;
//								key = new Key (ns, seqSet, keyString);
//							}
//						} 
//
//						client.Operate (null, key, Operation.Put (new Bin (accDayBinName, accDayString)), 
//							Operation.Put (new Bin (keyBinName, keyString)), 
//							Operation.Put (new Bin (seqBinName, seq)), 
//							ListOperation.Append (listBinName, Value.Get (eventString)));
					} 
					/*
					 * write to file
					 */
					file.WriteLine (account + "," + eventString);
					

				}
				//file.close ();
			}
		}

		public static void WriteEventToAerospike(string date, AerospikeClient client, string account, string product, string txType){
			/*
			Event record schema

			Key: account::date::sequence
			Bin: keyBinName - contains string same as key (diagnostic only)
			Bin: accBinName - contains string:  account 
			Bin: dayBinName - contains string:  day 
			Bin: seqBinName - contains integer sequence number
			Bin: listBinName - contains a CDT list of events
		
			CDT list element: product,txType
			*/
			int seq = 1;
			int eventSize = 0;

			string accDayString = account + "::" + date;
			string keyString = accDayString + "::" + seq;
			string eventString = product + "," + txType;
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

			client.Operate (null, key, 
				Operation.Put (new Bin (keyBinName, keyString)), 
				Operation.Put (new Bin (accBinName, account)), 
				Operation.Put (new Bin (dayBinName, date)), 
				Operation.Put (new Bin (seqBinName, seq)), 
				ListOperation.Append (listBinName, Value.Get (eventString)));
		}
		#endregion

		#region utility
		public static Dictionary<string, Object> makeKeyMap (Object key)
		{
			Dictionary<string, Object> map = new Dictionary<string, Object> ();
			map [LDT_KEY] = key;
			return map;
		}

		public static Dictionary<string, Object> makeValueMap (Object key, Object value)
		{
			Dictionary<string, Object> map = new Dictionary<string, Object> ();
			map [LDT_KEY] = key;
			map [LDT_VALUE] = value;
			return map;
		}

		public static Value makeValue (Object key, Object value)
		{
			return Value.Get (makeValueMap (key, value));
		}
		#endregion
	}

}
