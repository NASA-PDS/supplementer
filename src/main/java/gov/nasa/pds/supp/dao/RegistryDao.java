package gov.nasa.pds.supp.dao;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.gson.Gson;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Data Access Object (DAO) to work with Elasticsearch registry index.
 *  
 * @author karpenko
 */
public class RegistryDao extends Dao
{
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public RegistryDao(RestClient client, String esIndex)
    {
        super(client, esIndex);
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
        Request req = new Request("GET", "/" + indexName + "/_search");
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

    
    /**
     * Find existing lidvids
     * @param lidvids collection of lids
     * @return a list of existing lidvids
     * @throws Exception an exception
     */
    public Set<String> findExistingLidVids(Collection<String> lidvids) throws Exception
    {
        if(lidvids == null || lidvids.isEmpty()) return null;
        
        // Create request
        Request req = new Request("GET", "/" + indexName + "/_search");
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String json = bld.createFindLidVids(lidvids);
        req.setJsonEntity(json);
        
        // Execute request
        Response resp = client.performRequest(req);
        SearchResponseParser respParser = new SearchResponseParser();
        
        Set<String> existingIds = new TreeSet<>();
        
        respParser.parseResponse(resp, (id, rec) -> 
        {
            existingIds.add(id);
        });

        return existingIds;
    }

    
    /**
     * Call Elasticsearch bulk API to update multiple documents at once.
     * @param json JSON request
     * @throws Exception an exception
     */
    public void bulkUpdate(String json) throws Exception
    {
        Request req = new Request("POST", "/" + indexName + "/_bulk");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);

        String respJson = getLastLine(resp.getEntity().getContent());
        log.debug(respJson);
        
        if(responseHasErrors(respJson))
        {
            throw new Exception("Could not load data.");
        }

    }


    /**
     * This method is used to parse multi-line Elasticsearch error responses.
     * JSON error response is on the last line of a message.
     * @param is input stream
     * @return Last line
     */
    private static String getLastLine(InputStream is)
    {
        String lastLine = null;

        try
        {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));

            String line;
            while((line = rd.readLine()) != null)
            {
                lastLine = line;
            }
        }
        catch(Exception ex)
        {
            // Ignore
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return lastLine;
    }

    
    /**
     * Check for update errors.
     * @param resp Elasticsearch API response
     * @return true if there were any errors.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean responseHasErrors(String resp)
    {
        try
        {
            // Parse JSON response
            Gson gson = new Gson();
            Map json = (Map)gson.fromJson(resp, Object.class);
            
            Boolean hasErrors = (Boolean)json.get("errors");
            if(hasErrors)
            {
                List<Object> list = (List)json.get("items");
                
                // List size = batch size (one item per document)
                // NOTE: Only few items in the list could have errors
                for(Object item: list)
                {
                    Map index = (Map)((Map)item).get("update");
                    if(index == null) continue;
                    
                    Map error = (Map)index.get("error");
                    if(error != null)
                    {
                        String message = (String)error.get("reason");
                        log.error(message);
                        return true;
                    }
                }
                
                return true;
            }

            return false;
        }
        catch(Exception ex)
        {
            return false;
        }
    }

}
