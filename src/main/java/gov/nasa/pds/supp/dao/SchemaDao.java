package gov.nasa.pds.supp.dao;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.supp.util.Tuple;


/**
 * Elasticsearch schema DAO (Data Access Object).
 * This class provides methods to read and update Elasticsearch schema.
 * 
 * @author karpenko
 */
public class SchemaDao
{
    private RestClient client;
    private String esIndex;
    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public SchemaDao(RestClient client, String esIndex)
    {
        this.client = client;
        this.esIndex = esIndex;
    }
    
    
    /**
     * Get names of supplemental fields from Elasticsearch schema
     * @return a list of field names
     * @throws Exception an exception
     */
    public Set<String> getSupplementalFieldNames() throws Exception
    {
        Request req = new Request("GET", "/" + esIndex + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(esIndex);
        
        Set<String> fields = new TreeSet<>();
        parser.parse(resp.getEntity(), (name) -> 
        {
            if(name.startsWith("ops:Supplemental/"))
            {
                fields.add(name);
            }
        });
        
        return fields;
    }

    
    /**
     * Add new fields to Elasticsearch schema.
     * @param indexName Elasticsearch index to update, e.g., "registry".
     * @param fields A list of fields to add. Each field tuple has a name and a data type.
     * @throws Exception an exception
     */
    public void updateSchema(List<Tuple> fields) throws Exception
    {
        if(fields == null || fields.isEmpty()) return;
        
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + esIndex + "/_mapping");
        req.setJsonEntity(json);
        client.performRequest(req);
    }

}
