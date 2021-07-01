package gov.nasa.pds.supp.dao;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;

import com.google.gson.stream.JsonWriter;


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
}
