package gov.nasa.pds.supp.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.label.object.TableRecord;
import gov.nasa.pds.supp.dao.BulkUpdateJsonBuilder;
import gov.nasa.pds.supp.dao.DaoManager;
import gov.nasa.pds.supp.dao.RegistryDao;

/**
 * Loads supplemental data from a table data file.
 * @author karpenko
 */
public class SupplementalDataLoader
{
    private Logger log;
    private int UPDATE_BATCH_SIZE = 50;
    
    private BulkUpdateJsonBuilder bulkJsonBld;
    private int updatedRecordCount;


    /**
     * A Table record of supplemental data.
     * NOTE: We could not cache TableRecord instances. 
     * It is get overwritten on each TableObject.readNext() call and 
     * there is no clone() or copy constructor.
     */
    private static class Record
    {
        public String id;
        public String json;
    }
    
    
    /**
     * Constructor
     */
    public SupplementalDataLoader()
    {
        log = LogManager.getLogger(this.getClass());
        bulkJsonBld = new BulkUpdateJsonBuilder();
    }
    
    
    /**
     * Load supplemental data from a table data file.
     * @param table Table data file (referenced in PDS4 supplemental label XML)
     * @param esFieldInfo Information about data table columns / fields
     * @throws Exception an exception
     */
    public void loadData(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
        updatedRecordCount = 0;
        
        // LIDs
        if(esFieldInfo.lidIndex != 0)
        {
            while(loadLidDataBatch(table, esFieldInfo))
            {
                if(updatedRecordCount % 1000 == 0 && updatedRecordCount != 0)
                {
                    log.info("Updated " + updatedRecordCount + " record(s)");
                }
            }
        }
        // LIDVIDs
        else if(esFieldInfo.lidVidIndex != 0)
        {
            while(loadLidVidDataBatch(table, esFieldInfo))
            {
                if(updatedRecordCount % 1000 == 0 && updatedRecordCount != 0)
                {
                    log.info("Updated " + updatedRecordCount + " record(s)");
                }
            }
        }
        
        log.info("Updated " + updatedRecordCount + " record(s)");
    }

    
    private boolean loadLidDataBatch(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
        RegistryDao dao = DaoManager.getInstance().getRegistryDao();
        
        StringBuilder json = new StringBuilder();
        int jsonRecordCount = 0;
        
        // Read next batch
        List<Record> records = readNextBatch(table, esFieldInfo, UPDATE_BATCH_SIZE);
        if(records.isEmpty()) return false;
        
        // Get list of LIDs for this batch
        List<String> lids = new ArrayList<>();
        for(Record rec: records)
        {
            lids.add(rec.id);
        }

        // Get vids for batch lids from Elasticsearch
        Map<String, List<String>> vidMap = dao.findVidsByLids(lids);

        // Create JSON for Elasticsearch bulk update API call
        for(Record rec: records)
        {
            List<String> vids = vidMap.get(rec.id);
            if(vids == null)
            {
                log.warn("Skipping unregistered product " + rec.id);
                continue;
            }
            
            // If there are multiple versions of this LID, reuse the same data line (JSON)
            for(String vid: vids)
            {
                String lidvid = rec.id + "::" + vid;
                String pkJson = bulkJsonBld.createUpdatePK(lidvid);
                
                // NJSON (New Line Delimited JSON) format:
                // Line 1: primary key / id
                // Line 2: data
                json.append(pkJson);
                json.append("\n");
                json.append(rec.json);
                json.append("\n");
                
                jsonRecordCount++;
            }
        }
        
        // Update Elasticsearch registry index
        if(jsonRecordCount != 0)
        {
            dao.bulkUpdate(json.toString());
            updatedRecordCount += jsonRecordCount;
        }
        
        return records.size() == UPDATE_BATCH_SIZE;
    }
    

    private List<Record> readNextBatch(TableObject table, 
            SupplementalFieldsInfo esFieldInfo, int batchSize) throws Exception
    {
        List<Record> records = new ArrayList<>(batchSize);
        for(int i = 0; i < batchSize; i++)
        {
            TableRecord trec = table.readNext();
            if(trec == null) break;
            
            Record rec = new Record();
            // Create data JSON
            rec.json = bulkJsonBld.createUpdateJson(esFieldInfo, trec);

            // LID
            if(esFieldInfo.lidIndex != 0)
            {
                rec.id = trec.getString(esFieldInfo.lidIndex).trim(); 
            }
            
            // LIDVID overwrites LID
            if(esFieldInfo.lidVidIndex != 0)
            {
                rec.id = trec.getString(esFieldInfo.lidVidIndex).trim(); 
            }
            
            records.add(rec);
        }
        
        return records;
    }
    
    
    private boolean loadLidVidDataBatch(TableObject table, SupplementalFieldsInfo esFieldInfo) throws Exception
    {
        RegistryDao dao = DaoManager.getInstance().getRegistryDao();
        
        StringBuilder json = new StringBuilder();
        int jsonRecordCount = 0;
        
        // Read next batch
        List<Record> records = readNextBatch(table, esFieldInfo, UPDATE_BATCH_SIZE);
        if(records.isEmpty()) return false;
        
        // Get list of LIDVIDs for this batch
        List<String> lidvids = new ArrayList<>();
        for(Record rec: records)
        {
            lidvids.add(rec.id);
        }

        // Get existing LIDVIDs for this batch from Elasticsearch
        Set<String> existingIds = dao.findExistingLidVids(lidvids);

        // Create JSON for Elasticsearch bulk update API call
        for(Record rec: records)
        {
            if(!existingIds.contains(rec.id))
            {
                log.warn("Skipping unregistered product " + rec.id);
                continue;
            }
            
            // NJSON (New Line Delimited JSON) format:
            // Line 1: primary key / id
            // Line 2: data
            String pkJson = bulkJsonBld.createUpdatePK(rec.id);
            json.append(pkJson);
            json.append("\n");
            json.append(rec.json);
            json.append("\n");
            
            jsonRecordCount++;
        }
        
        // Update Elasticsearch registry index
        if(jsonRecordCount != 0)
        {
            dao.bulkUpdate(json.toString());
            updatedRecordCount += jsonRecordCount;
        }
        
        return records.size() == UPDATE_BATCH_SIZE;
    }

}
