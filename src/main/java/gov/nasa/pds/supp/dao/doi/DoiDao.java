package gov.nasa.pds.supp.dao.doi;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.common.es.dao.BulkResponseParser;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.dao.Dao;
import gov.nasa.pds.supp.dao.RegistryRequestBuilder;

/**
 * Data access object to query and update DOI information in Elasticsearch
 * 
 * @author karpenko
 */
public class DoiDao extends Dao
{
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public DoiDao(RestClient client, String esIndex)
    {
        super(client, esIndex);
    }


    /**
     * Get DOIs by primary key 
     * @param ids primary keys (usually LIDVIDs)
     * @return map: key = product primary key (usually LIDVID), value = set of DOIs 
     * @throws Exception an exception
     */
    public Map<String, Set<String>> getDois(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;
        
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String jsonReq = bld.createGetDoisRequest(ids);
        
        String reqUrl = "/" + indexName + "/_search";
        if(pretty) reqUrl += "?pretty";
        
        Request req = new Request("GET", reqUrl);
        req.setJsonEntity(jsonReq);
        Response resp = client.performRequest(req);

        //DebugUtils.dumpResponseBody(resp);
        
        GetDoisParser cb = new GetDoisParser();
        SearchResponseParser parser = new SearchResponseParser();
        parser.parseResponse(resp, cb);
        
        return cb.getDoiMap();
    }

    
    /**
     * Update alternate IDs by primary keys
     * @param newIds ID map: key = product primary key (usually LIDVID), 
     * value = additional alternate IDs to be added to existing alternate IDs.
     * @throws Exception an exception
     */
    public void updateDois(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) return;
        
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String json = bld.createUpdateDoisRequest(newIds);
        log.debug("Request:\n" + json);
        
        String reqUrl = "/" + indexName + "/_bulk"; //?refresh=wait_for";
        Request req = new Request("POST", reqUrl);
        req.setJsonEntity(json);
        
        Response resp = client.performRequest(req);
        
        // Check for Elasticsearch errors.
        InputStream is = null;
        InputStreamReader rd = null;
        try
        {
            is = resp.getEntity().getContent();
            rd = new InputStreamReader(is);
            
            BulkResponseParser parser = new BulkResponseParser();
            parser.parse(rd);
        }
        finally
        {
            CloseUtils.close(rd);
            CloseUtils.close(is);
        }
    }

}
