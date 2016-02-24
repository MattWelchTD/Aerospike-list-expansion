
-- purge function checks that 
-- the sequence number is 1
-- and all the elements in the CDT list are cancelled
function purge(topRec)
	if aerospike:exists(topRec, listBinName, seqBinName) then
		if topRec[seqBinName] < 2 then
			local onlyCancelled = false
			for value in list.iterator[listBinName] do
				if not string.find (value, "cancelled") then
					onlyCancelled = true
					break
				end
			end
			if onlyCancelled then
				aerospike:remove(topRec)
			end
		end
	end
end