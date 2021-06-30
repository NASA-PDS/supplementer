package gov.nasa.pds.supp.dao;

import org.elasticsearch.client.RestClient;

public final class DaoManager
{
    private static DaoManager instance;
    
    private RegistryDao registryDao;
    private SchemaDao schemaDao;
    
    
    private DaoManager(RestClient client, String esIndex)
    {
        registryDao = new RegistryDao(client, esIndex);
        schemaDao = new SchemaDao(client, esIndex);
    }
    
    
    public static DaoManager getInstance()
    {
        return instance;
    }
    
    
    public static void init(RestClient client, String esIndex)
    {
        instance = new DaoManager(client, esIndex);
    }
}
