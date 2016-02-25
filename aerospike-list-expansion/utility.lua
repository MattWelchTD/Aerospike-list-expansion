
-- purge function checks that 
-- the listBinName is equal to the target date
-- and delets the record
function purge(topRec, listBinName, date)
	if aerospike:exists(topRec) and topRec[listBinName] == date then
		aerospike:remove(topRec)
	end
end