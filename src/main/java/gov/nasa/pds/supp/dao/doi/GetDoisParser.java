package gov.nasa.pds.supp.dao.doi;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.supp.Constants;

/**
 * Parses "get DOIs" response from Elasticsearch.
 * @author karpenko
 */
public class GetDoisParser implements SearchResponseParser.Callback
{
    private Map<String, Set<String>> map;
    
    /**
     * Constructor
     */
    public GetDoisParser()
    {
        map = new TreeMap<>();
    }

    
    /**
     * Get DOIs by primary key (usually LIDVID)
     * @return a map. Key = primary key (usually LIDVID), Value = a set of DOIs
     */
    public Map<String, Set<String>> getDoiMap()
    {
        return map;
    }
    
    
    @Override
    public void onRecord(String id, Object rec) throws Exception
    {
        if(rec instanceof Map<?, ?>)
        {
            Object obj = ((Map<?, ?>)rec).get(Constants.DOI_FIELD);
            
            // No DOIs
            if(obj == null) 
            {
                map.put(id, new TreeSet<>());
            }
            // Multiple values
            else if(obj instanceof List<?>)
            {
                Set<String> dois = new TreeSet<>();
                for(Object item: (List<?>)obj)
                {
                    dois.add(item.toString());
                }
                
                map.put(id, dois);
            }
            // Single value
            else if(obj instanceof String)
            {
                Set<String> dois = new TreeSet<>();
                dois.add((String)obj);
                map.put(id, dois);
            }
        }
    }

}
