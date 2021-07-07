package gov.nasa.pds.supp.dao;

import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.label.object.TableRecord;
import gov.nasa.pds.registry.common.util.date.PdsDateConverter;
import gov.nasa.pds.supp.cmd.SupplementalFieldsInfo;

/**
 * Builds JSON for Elasticsearch Bulk Update API call. 
 * 
 * @author karpenko
 */
public class BulkUpdateJsonBuilder
{
    PdsDateConverter dateConv = new PdsDateConverter(false);
    
    /**
     * Create primary key JSON for bulk update request
     * @param id primary key
     * @return JSON
     * @throws Exception an exception
     */
    public String createUpdatePK(String id) throws Exception
    {
        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);
        
        jw.beginObject();
        
        jw.name("update");
        jw.beginObject();
        jw.name("_id").value(id);
        jw.endObject();
        
        jw.endObject();
        
        jw.close();        
        return sw.getBuffer().toString();
    }

    
    /**
     * Create bulk update API data line (line 2) of 2 line NJSON record.
     * @param esFieldInfo Information about data table columns / fields
     * @param rec a record from table data
     * @return JSON
     * @throws Exception an exception
     */
    public String createUpdateJson(SupplementalFieldsInfo esFieldInfo, TableRecord rec) throws Exception
    {
        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);

        jw.beginObject();
        jw.name("doc");
        jw.beginObject();

        // Index starts from 1
        for(int i = 1; i< esFieldInfo.size(); i++)
        {
            String fieldName = esFieldInfo.getName(i);
            String dataType = esFieldInfo.getDataType(i);
            
            // LID / LIDVID
            if(dataType == null)
            {
                continue;
            }
            
            String fieldValue = rec.getString(i).trim();
            
            // Convert dates to "ISO instant" format
            if("date".equals(dataType))
            {
                fieldValue = dateConv.toIsoInstantString(fieldName, fieldValue);
            }
            
            jw.name(fieldName).value(fieldValue);
        }
        
        jw.endObject();
        jw.endObject();
        
        jw.close();        
        return sw.getBuffer().toString();
    }
    
}
