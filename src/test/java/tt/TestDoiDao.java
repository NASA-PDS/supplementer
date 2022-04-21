package tt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.dao.doi.DoiDao;


public class TestDoiDao
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient("localhost", null);
            DoiDao dao = new DoiDao(client, "registry");

            List<String> lids = Arrays.asList(
                    "urn:nasa:pds:kaguya_grs_spectra:document:kgrs_ephemerides_doc::1.0",
                    "urn:nasa:pds:cassini_vims_cruise:data_raw:1294638377");
            Map<String, Set<String>> map = dao.getDois(lids);
            map.forEach((key, value) -> 
            { 
                System.out.println(key + ": " + value); 
            });
            
            System.out.println("Ok");
        }
        finally
        {
            CloseUtils.close(client);
        }

    }

}
