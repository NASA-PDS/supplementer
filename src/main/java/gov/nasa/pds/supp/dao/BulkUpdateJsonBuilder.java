package gov.nasa.pds.supp.dao;

import java.io.StringWriter;

import com.google.gson.stream.JsonWriter;

/**
 * Builds JSON for Elasticsearch Bulk Update API call. 
 * 
 * @author karpenko
 */
public class BulkUpdateJsonBuilder
{
    /**
     * Create primary key JSON for bulk update request
     * @param id primary key
     * @return JSON
     * @throws Exception an exception
     */
    public static String createUpdatePK(String id) throws Exception
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

    
    
}
