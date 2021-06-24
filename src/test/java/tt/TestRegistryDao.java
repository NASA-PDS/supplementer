package tt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.dao.RegistryDao;

public class TestRegistryDao
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient("localhost", null);
            RegistryDao dao = new RegistryDao(client);

            List<String> lids = Arrays.asList("urn:nasa:pds:cassini_vims_cruise:data_raw:1294638283");
            Map<String, List<String>> map = dao.findLidVidsByLids(lids);
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

}
