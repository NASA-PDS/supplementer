package gov.nasa.pds.supp.dao;

import org.elasticsearch.client.RestClient;

/**
 * A singleton to store DAO references.
 * 
 * @author karpenko
 */
public final class DaoManager
{
    private static DaoManager instance;
    
    private RegistryDao registryDao;
    private SchemaDao schemaDao;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    private DaoManager(RestClient client, String esIndex)
    {
        registryDao = new RegistryDao(client, esIndex);
        schemaDao = new SchemaDao(client, esIndex);
    }
    
    
    /**
     * Get singleton instance
     * @return singleton instance
     */
    public static DaoManager getInstance()
    {
        return instance;
    }
    
    
    /**
     * Init DAO manager
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public static void init(RestClient client, String esIndex)
    {
        instance = new DaoManager(client, esIndex);
    }
    
    
    /**
     * Get RegistryDao
     * @return RegistryDao
     */
    public RegistryDao getRegistryDao()
    {
        return registryDao;
    }
    

    /**
     * Get SchemaDao
     * @return SchemaDao
     */
    public SchemaDao getSchemaDao()
    {
        return schemaDao;
    }

}
