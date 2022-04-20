package gov.nasa.pds.supp.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

/**
 * Base data access object (DAO)
 * @author karpenko
 *
 */
public class Dao
{
    protected Logger log;
    
    protected RestClient client;
    protected String indexName;
    protected boolean pretty = false;

    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param esIndex Elasticsearch index name
     */
    public Dao(RestClient client, String esIndex)
    {
        this.client = client;
        this.indexName = esIndex;
        
        log = LogManager.getLogger(this.getClass());
    }

}
