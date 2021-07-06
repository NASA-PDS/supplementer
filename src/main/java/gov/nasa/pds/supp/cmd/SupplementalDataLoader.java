package gov.nasa.pds.supp.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.label.object.TableRecord;
import gov.nasa.pds.supp.dao.BulkUpdateJsonBuilder;
import gov.nasa.pds.supp.dao.DaoManager;
import gov.nasa.pds.supp.dao.RegistryDao;

public class SupplementalDataLoader
{
    private Logger log;
    
    
    public SupplementalDataLoader()
    {
        log = LogManager.getLogger(this.getClass());        
    }
    
    
    public void loadData(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
        if(esFieldInfo.lidIndex != 0)
        {
            loadLidData(table, esFieldInfo);
        }
        else if(esFieldInfo.lidVidIndex != 0)
        {
            loadLidVidData(table, esFieldInfo);
        }
    }

    
    private void loadLidData(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
        RegistryDao dao = DaoManager.getInstance().getRegistryDao();
        
        // Read next batch
        List<TableRecord> records = readNextBatch(table, 1);
        if(records.isEmpty()) return;
        
        // Get list of LIDs for this batch
        List<String> lids = new ArrayList<>();
        for(TableRecord rec: records)
        {
            String lid = rec.getString(esFieldInfo.lidIndex).trim();
            lids.add(lid);
        }

        // Get vids for batch lids from Elasticsearch
        Map<String, List<String>> vidMap = dao.findVidsByLids(lids);

        // Create JSON for Elasticsearch bulk update API call
        for(TableRecord rec: records)
        {
            String lid = rec.getString(esFieldInfo.lidIndex).trim();
            List<String> vids = vidMap.get(lid);
            if(vids == null)
            {
                log.warn("Skipping unregistered product " + lid);
                continue;
            }
            
            for(String vid: vids)
            {
                String lidvid = lid + "::" + vid;
                String pkJson = BulkUpdateJsonBuilder.createUpdatePK(lidvid);
                System.out.println(pkJson);
            }
        }
    }
    

    private List<TableRecord> readNextBatch(TableObject table, int batchSize) throws Exception
    {
        List<TableRecord> records = new ArrayList<>(batchSize);
        for(int i = 0; i < batchSize; i++)
        {
            TableRecord rec = table.readNext();
            if(rec == null) break;
            records.add(rec);
        }
        
        return records;
    }
    
    
    private void loadLidVidData(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
    }

}
