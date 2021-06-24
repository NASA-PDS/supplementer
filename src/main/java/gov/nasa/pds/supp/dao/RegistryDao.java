package gov.nasa.pds.supp.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.client.RestClient;

/**
 * Data Access Object (DAO) to work with Elasticsearch registry index.
 *  
 * @author karpenko
 */
public class RegistryDao
{
    private RestClient client;
    
    /**
     * Constructor
     * @param client Elasticsearch client
     */
    public RegistryDao(RestClient client)
    {
        this.client = client;
    }

    
    /**
     * Find lidvids by lids
     * @param lids lids
     * @return a map where key = lid, value = list of lidvids
     */
    public Map<String, List<String>> findLidVidsByLids(Collection<String> lids)
    {
        if(lids == null || lids.isEmpty()) return null;
        
        Map<String, List<String>> map = new TreeMap<>();
        
        
        return map;
    }
}
