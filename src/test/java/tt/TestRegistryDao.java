package tt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.dao.RegistryDao;
import gov.nasa.pds.supp.dao.SchemaDao;

public class TestRegistryDao
{

    public static void main(String[] args) throws Exception
    {
        testGetSupFieldNames();
    }
    
    
    public static void testFindVidsByLids() throws Exception
    {
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient("localhost", null);
            RegistryDao dao = new RegistryDao(client, "registry");

            List<String> lids = Arrays.asList(
                    "urn:nasa:pds:cassini_vims_cruise:data_raw:1294638283",
                    "urn:nasa:pds:cassini_vims_cruise:data_raw:1294638377");
            Map<String, List<String>> map = dao.findVidsByLids(lids);
            map.forEach((key, value) -> 
            { 
                System.out.println(key + ": " + value); 
            });
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    public static void testGetSupFieldNames() throws Exception
    {
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient("localhost", null);
            SchemaDao dao = new SchemaDao(client, "registry");
            Set<String> fields = dao.getSupplementalFieldNames();
            
            for(String field: fields)
            {
                System.out.println(field);
            }
        }
        finally
        {
            CloseUtils.close(client);
        }
    }
    
}
