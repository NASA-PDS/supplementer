package gov.nasa.pds.supp.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;

/**
 * Data Access Object (DAO) to work with Elasticsearch registry index.
 *  
 * @author karpenko
 */
public class RegistryDao
{
    private RestClient client;
    private String esIndex;
    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public RegistryDao(RestClient client, String esIndex)
    {
        this.client = client;
        this.esIndex = esIndex;
    }

    
    /**
     * Find vids by lids
     * @param lids collection of lids
     * @return a map where key = lid, value = list of vids
     * @throws Exception an exception
     */
    public Map<String, List<String>> findVidsByLids(Collection<String> lids) throws Exception
    {
        if(lids == null || lids.isEmpty()) return null;
        
        // Max number of records to return
        // TODO: Add pagination to return all records?
        // We plan to use batches of 100 lids, so we can handle up to 50 vids per lid.
        int maxHits = 5000;
        
        // Create request
        Request req = new Request("GET", "/" + esIndex + "/_search");
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String json = bld.createFindVidsByLids(lids, maxHits);
        req.setJsonEntity(json);
        
        // Execute request
        Response resp = client.performRequest(req);
        SearchResponseParser respParser = new SearchResponseParser();
        
        Map<String, List<String>> map = new TreeMap<>();
        
        respParser.parseResponse(resp, (id, rec) -> 
        {
            int idx = id.lastIndexOf("::");
            if(idx > 0)
            {
                String lid = id.substring(0, idx);
                String vid = id.substring(idx+2);
                
                List<String> vids = map.get(lid);
                if(vids == null) 
                {
                    vids = new ArrayList<>();
                    map.put(lid, vids);
                }
                
                vids.add(vid);
            }
        });
                
        return map;
    }
}
