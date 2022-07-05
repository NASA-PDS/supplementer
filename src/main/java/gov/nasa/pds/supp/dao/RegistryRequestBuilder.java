package gov.nasa.pds.supp.dao;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.supp.Constants;


/**
 * A class to build Elasticsearch API JSON requests.
 * 
 * @author karpenko
 */
public class RegistryRequestBuilder
{
    private boolean pretty;

    
    /**
     * Constructor
     * @param pretty Pretty-format JSON requests
     */
    public RegistryRequestBuilder(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    /**
     * Constructor
     */
    public RegistryRequestBuilder()
    {
        this(false);
    }

    
    private JsonWriter createJsonWriter(Writer writer)
    {
        JsonWriter jw = new JsonWriter(writer);
        if (pretty)
        {
            jw.setIndent("  ");
        }

        return jw;
    }

    
    /**
     * Create Elasticsearch query to find documents (lidvids) by lids.
     * @param lids one or more LIDs
     * @param maxHits max number of results to return
     * @return Elasticsearch JSON query
     * @throws Exception an exception
     */
    public String createFindVidsByLids(Collection<String> lids, int maxHits) throws Exception
    {
        if(lids == null || lids.isEmpty()) return null;
        
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        writer.name("_source").value(false);
        writer.name("size").value(maxHits);
        
        writer.name("query");
        writer.beginObject();
        
        writer.name("terms");
        writer.beginObject();
        
        writer.name("lid");
        writer.beginArray();
        for(String lid: lids)
        {
            writer.value(lid);
        }
        writer.endArray();
        
        writer.endObject();     // terms        
        writer.endObject();     // query
        
        writer.endObject();
        writer.close();
        return out.toString();
    }

    
    /**
     * Create Elasticsearch query to find existing lidvids.
     * @param lidvids one or more LIDs
     * @return Elasticsearch JSON query
     * @throws Exception an exception
     */
    public String createFindLidVids(Collection<String> lidvids) throws Exception
    {
        if(lidvids == null || lidvids.isEmpty()) return null;
        
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        writer.name("_source").value(false);
        writer.name("size").value(lidvids.size());
        
        writer.name("query");
        writer.beginObject();
        
        writer.name("terms");
        writer.beginObject();
        
        writer.name("lidvid");
        writer.beginArray();
        for(String id: lidvids)
        {
            writer.value(id);
        }
        writer.endArray();
        
        writer.endObject();     // terms        
        writer.endObject();     // query
        
        writer.endObject();
        writer.close();
        return out.toString();
    }

    
    /**
     * Build a query to select DOIs by document primary key
     * @param ids list of primary keys (lidvids right now)
     * @return JSON
     * @throws Exception an exception
     */
    public String createGetDoisRequest(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) throw new Exception("Missing ids");
            
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        // Create ids query
        writer.beginObject();

        writer.name("_source").value(Constants.DOI_FIELD);
        writer.name("size").value(ids.size());

        writer.name("query");
        writer.beginObject();
        writer.name("ids");
        writer.beginObject();
        
        writer.name("values");
        writer.beginArray();
        for(String id: ids)
        {
            writer.value(id);
        }
        writer.endArray();
        
        writer.endObject();
        writer.endObject();
        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    /**
     * Create Elasticsearch request to update DOI field(s)
     * @param doiMap key = primary keys (usually LIDVIDs), value = set of DOIs
     * @return JSON string
     * @throws Exception an exception
     */
    public String createUpdateDoisRequest(Map<String, Set<String>> doiMap) throws Exception
    {
        if(doiMap == null || doiMap.isEmpty()) throw new IllegalArgumentException("Missing ids");
        
        StringBuilder bld = new StringBuilder();
        
        // Build NJSON (new-line delimited JSON)
        for(Map.Entry<String, Set<String>> entry: doiMap.entrySet())
        {
            // Line 1: Elasticsearch document ID
            bld.append("{ \"update\" : {\"_id\" : \"" + entry.getKey() + "\" } }\n");
            
            // Line 2: Data
            String dataJson = buildUpdateDocJson(Constants.DOI_FIELD, entry.getValue());
            bld.append(dataJson);
            bld.append("\n");
        }
        
        return bld.toString();

    }

    
    private String buildUpdateDocJson(String field, Collection<String> values) throws Exception
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        writer.name("doc");
        writer.beginObject();
        
        writer.name(field);
        
        writer.beginArray();        
        for(String value: values)
        {
            writer.value(value);
        }
        writer.endArray();
        
        writer.endObject();        
        writer.endObject();
        
        writer.close();
        return out.toString();
    }

}
