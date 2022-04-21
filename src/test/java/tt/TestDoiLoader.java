package tt;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.cmd.doi.DoiLoader;
import gov.nasa.pds.supp.dao.DaoManager;
import gov.nasa.pds.supp.dao.doi.SqliteReader;
import gov.nasa.pds.supp.util.log.Log4jConfigurator;


public class TestDoiLoader
{
    private static class MyCallback implements SqliteReader.Callback
    {
        @Override
        public void onBatch(Map<String, Set<String>> batch)
        {
            System.out.println("===============================");
            batch.forEach((id, dois) -> 
            {
                //if(!id.startsWith("urn:nasa"))
                System.out.println(id + ", " + dois);
            });
        }
    }
    
    
    private static void testDbFile() throws Exception
    {
        SqliteReader reader = new SqliteReader(new MyCallback());
        reader.read("C:/tmp/doi_2.db");
    }
    
    
    public static void testDoiLoader() throws Exception
    {
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient("localhost", null);
            DaoManager.init(client, "registry");
            
            DoiLoader loader = new DoiLoader();
            loader.load(new File("C:/tmp/doi_2.db"));
        }
        finally
        {
            CloseUtils.close(client);
        }
    }
    

    public static void main(String[] args) throws Exception
    {
        Log4jConfigurator.configure("INFO", null);
        
        testDoiLoader();
    }

    
}
