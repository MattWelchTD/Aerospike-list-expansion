using System;
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Helper.Collection
{
	public sealed class LargeList
	{
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
		public void Add(Value value)
		{
				
			String subKeyString = account + ":" + product;
			Key subKey = new Key (ns, cdtSet, subKeyString);

			client.Operate(null, subKey, 
				Operation.Add(new Bin(productPositionBinName, productAmount)), 
				Operation.Put(new Bin(keyBinName, subKeyString)),
				Operation.Put(new Bin(accBinName, account)),
				Operation.Put(new Bin(prodNameBinName, product))
			);

			// add the digest of the subKey to the CDT List in the Customer record
			client.Operate(null, cdtkey, ListOperation.Append(binNameString, Value.Get(subKey.digest)));
		}
		public void Add(List<Value> items)
		{
		}

		public void Add(params Value[] items)
		{
		}
		public void Update(Value value)
		{
			
		}
		public void Update(params Value[] values)
		{
			
		}

		public void Update(IList values)
		{
			
		}

		public void Remove(Value value)
		{
			
		}

		public void Remove(IList values)
		{
			
		}
		public int Remove(Value begin, Value end)
		{
			object result = client.Execute(policy, key, PackageName, "remove_range", binName, begin, end);
			return (result != null) ? (int)(long)result : 0;
		}
		public bool Exists(Value keyValue)
		{
			IList list = (IList)client.Execute(policy, key, PackageName, "exists", binName, keyValue);
			return (list != null)? Util.ToBool(list[0]) : false;
		}

		public IList<bool> Exists(IList keyValues)
		{
			IList list = (IList)client.Execute(policy, key, PackageName, "exists", binName, Value.Get(keyValues));

			if (list != null)
			{
				IList<bool> target = new List<bool>(list.Count);

				foreach (object obj in list)
				{
					target.Add(Util.ToBool(obj));
				}
				return target;
			}
			else
			{
				int max = keyValues.Count;
				IList<bool> target = new List<bool>(max);

				for (int i = 0; i < max; i++)
				{
					target.Add(false);
				}
				return target;
			}
		}

		public IList Find(Value value)
		{
			return (IList)client.Execute(policy, key, PackageName, "find", binName, value);
		}

		public IList FindFrom(Value begin, int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count));
		}

		public IList FindFrom(Value begin, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		public IList Range(Value begin, Value end)
		{
			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end);
		}

		public IList Range(Value begin, Value end, int count)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count));
		}

		public IList Range(Value begin, Value end, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end, Value.Get(filterModule), Value.Get(filterModule), Value.Get(filterArgs));
		}

		public IList Range(Value begin, Value end, int count, string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		public IList Scan()
		{
			return (IList)client.Execute(policy, key, PackageName, "scan", binName);
		}

		public IList Filter(string filterModule, string filterName, params Value[] filterArgs)
		{
			return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.AsNull, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
		}

		public void Destroy()
		{
			client.Execute(policy, key, PackageName, "destroy", binName);
		}

		public int Size()
		{
			object result = client.Execute(policy, key, PackageName, "size", binName);
			return (result != null) ? (int)(long)result : 0;
		}

		public IDictionary GetConfig()
		{
			return (IDictionary)client.Execute(policy, key, PackageName, "config", binName);
		}

		public void SetPageSize(int pageSize)
		{
			// do nothing
		}

	}
}

