using System;
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Helper.Collection
{
	public sealed class LargeList
	{

		public const String ListElementBinName = "__ListElement";

		private readonly AerospikeClient client;
		private readonly WritePolicy policy;
		private readonly Key key;
		private readonly Value binName;
		private readonly String binNameString;

		public LargeList(AerospikeClient client, WritePolicy policy, Key key, string binName)
		{
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.binName = Value.Get(binName);
			this.binNameString = this.binName.ToString ();
		}

		private List<byte[]> SubRecordList(Key key){
			Record record = client.Get (this.policy, key, binNameString);
			if (record != null) {
				return (List<byte[]>)record.GetList (binNameString);
			}
			return null;
		}

		private Key MakeSubKey(Value value) {
			Key subKey;
			String valueString;
			if (value is Value.MapValue) {

				IDictionary map = (IDictionary) value.Object;
				valueString = map ["key"].ToString();

			} else {
				
				valueString = value.ToString ();

			}
			string subKeyString = String.Format ("{0}::{1}", this.key.userKey.ToString (), valueString);
			subKey = new Key (this.key.ns, this.key.setName, subKeyString);
			return subKey;
		}
		private Key[] MakeSubKeys(IList<Value> values) {
			Key[] keys = new Key[values.Count];
			int index = 0;
			foreach (Value value in values) {
				keys [index] = MakeSubKey (value);
				index++;
			}
			return keys;
		}

		private IList GetDigestList(){
			Record topRecord = client.Get (this.policy, this.key, this.binNameString);
			if (topRecord == null)
				return new List<byte[]> ();
			IList digestList = topRecord.GetList (this.binNameString);
			if (digestList == null)
				return new List<byte[]> ();
			return digestList;
		}

		public void Add(Value value)
		{
				
			Key subKey = MakeSubKey (value);

			client.Put (this.policy, subKey, new Bin (ListElementBinName, value));

			// add the digest of the subKey to the CDT List in the Customer record
			client.Operate(this.policy, this.key, ListOperation.Append(this.binNameString, Value.Get(subKey.digest)));

		}
		public void Add(List<Value> items)
		{
			foreach(Value Value in items){
				this.Add(Value);
			}
		}

		public void Add(params Value[] items)
		{
			foreach(Value Value in items){
				this.Add(Value);
			}
		}
		public void Update(Value value)
		{
			
			if (Size () == 0) {
				Add (value);
			} else {
				Key subKey = MakeSubKey (value);
				client.Put (this.policy, subKey, new Bin (ListElementBinName, value));
			}
		}
		public void Update(params Value[] values)
		{
			foreach(Value Value in values){
				this.Update(Value);
			}
		}

		public void Update(IList values)
		{
			foreach(Value Value in values){
				this.Update(Value);
			}

		}

		public void Remove(Value value)
		{
			Key subKey = MakeSubKey (value);
			IList digestList = GetDigestList ();
			int index = digestList.IndexOf (subKey.digest);
			client.Delete (this.policy, subKey);
			client.Operate(this.policy, this.key, ListOperation.Remove(this.binNameString, index));

		}

		public void Remove(IList<Value> values)
		{
			Key[] keys = MakeSubKeys (values);
			IList digestList = GetDigestList ();

//			int startIndex = digestList.IndexOf (subKey.digest);
//			int count = values.Count;
//			foreach (Key key in keys){
//				
//				client.Delete (this.policy, key);
//			}
//			client.Operate(this.policy, this.key, ListOperation.Remove(this.binNameString, startIndex, count));
		
			foreach (Key key in keys){

				client.Delete (this.policy, key);
				digestList.Remove (key.digest);

			}

			client.Put (this.policy, this.key, new Bin (this.binNameString, digestList));
			
		}

		public int Remove(Value begin, Value end)
		{
			IList digestList = GetDigestList ();
			Key beginKey = MakeSubKey (begin);
			Key endKey = MakeSubKey (end);
			int start = digestList.IndexOf (beginKey.digest);
			int stop = digestList.IndexOf (endKey.digest);
			int count = stop - start + 1;;
			for (int i = start; i < stop; i++){
				Key subKey = new Key (this.key.ns, (byte[])digestList [i], null, null);
				client.Delete (this.policy, subKey);
			}
			client.Operate(this.policy, this.key, ListOperation.RemoveRange(this.binNameString, start, count));
			return count;
		}
		public bool Exists(Value keyValue)
		{
			Key subKey = MakeSubKey (keyValue);
			return client.Exists (this.policy, subKey);

		}

		public IList<bool> Exists (IList keyValues)
		{
			IList<bool> target = new List<bool> ();
			foreach (Object value in keyValues) {
				target.Add (Exists (Value.Get(value)));
			}
			return target;

		}

		public IList Find(Value value)
		{
			Key subKey = MakeSubKey (value);
			Record record = client.Get (this.policy, subKey, ListElementBinName);
			if (record != null) {
				Object result = record.GetValue (ListElementBinName);
				return new List<Object> (){ result };
			} else {
				return null;
			}

		}

		private IList get(IList digestList, int start, int stop){
			List<Object> results = new List<object> ();

			for (int i = start; i < stop; i++) {
				Key subKey = new Key (this.key.ns, (byte[])digestList [i], null, null);
				Record record = client.Get (this.policy, subKey, ListElementBinName);
				Object result = record.GetValue (ListElementBinName);
				results.Add (result);
			}
			return results;
		}

		public IList FindFrom (Value begin, int count)
		{
			IList digestList = GetDigestList ();
			Key beginKey = MakeSubKey (begin);
			int start = digestList.IndexOf (beginKey.digest);
			int stop = start + count;
			return get (digestList, start, stop);
		}
//
//		public IList FindFrom(Value begin, int count, string filterModule, string filterName, params Value[] filterArgs)
//		{
//			return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
//		}

		public IList Range(Value begin, Value end)
		{
			IList digestList = GetDigestList ();
			Key beginKey = MakeSubKey (begin);
			Key endKey = MakeSubKey (end);
			int start = digestList.IndexOf (beginKey.digest);
			int stop = digestList.IndexOf (endKey.digest);
			return get (digestList, start, stop);
		}

//		public IList Range(Value begin, Value end, int count)
//		{
//			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count));
//		}
//
//		public IList Range(Value begin, Value end, string filterModule, string filterName, params Value[] filterArgs)
//		{
//			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end, Value.Get(filterModule), Value.Get(filterModule), Value.Get(filterArgs));
//		}
//
//		public IList Range(Value begin, Value end, int count, string filterModule, string filterName, params Value[] filterArgs)
//		{
//			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
//		}

		public IList Scan()
		{
			
			IList digestList = GetDigestList ();
			if (digestList != null || digestList.Count > 0) {
				IList results = new List<Value> ();
				foreach (byte[] digest in digestList) {
					Key subKey = new Key (this.key.ns, digest, null, null);
					Record record = client.Get (this.policy, subKey, this.binNameString);
					results.Add (record.GetValue (ListElementBinName));
				}
				return results;

			}
			return null;
		}

//		public IList Filter(string filterModule, string filterName, params Value[] filterArgs)
//		{
//			return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.AsNull, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
//		}

		public void Destroy ()
		{
			IList digestList = GetDigestList ();

			client.Put (this.policy, this.key, Bin.AsNull(this.binNameString));
						
			foreach (byte[] digest in digestList) {
				Key subKey = new Key (this.key.ns, digest, null, null);
				client.Delete (this.policy, subKey);
			}


		}

		public int Size()
		{
			Record record = client.Operate(this.policy, this.key, ListOperation.Size(this.binNameString));
			return record.GetInt ("Count");
		}

		public IDictionary GetConfig()
		{
			return null;  // No config because its NotSupportedException AsyncNode LDT
		}

		public void SetPageSize(int pageSize)
		{
			// do nothing
		}

	}
}

