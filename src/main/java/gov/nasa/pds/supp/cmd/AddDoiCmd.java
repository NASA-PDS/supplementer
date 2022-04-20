package gov.nasa.pds.supp.cmd;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.common.util.Tuple;
import gov.nasa.pds.supp.Constants;
import gov.nasa.pds.supp.dao.DaoManager;

/**
 * CLI command to update products already stored in Elasticsearch registry index  
 * with DOI data (from Sqlite database).
 *  
 * @author karpenko
 */
public class AddDoiCmd implements CliCommand
{
    /**
     * Constructor
     */
    public AddDoiCmd()
    {
    }
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        // Read Elasticsearch parameters
        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", "registry");
        String authPath = cmdLine.getOptionValue("auth");

        // Read and validate "-file" parameter
        String pFile = cmdLine.getOptionValue("file");
        if(pFile == null) throw new Exception("Missing required parameter '-file'");

        File file = new File(pFile);
        if(!file.exists()) throw new Exception("File doesn't exist: " + pFile);

        // Init Elasticsearch client and DAOs
        RestClient client = null;
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);
            DaoManager.init(client, indexName);
            
            // Update Elasticsearch schema if needed
            updateSchema(client, indexName);
            
            // Process Sqlite database file
            processFile(file);
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    private void updateSchema(RestClient client, String indexName) throws Exception
    {
        SchemaDao dao = new SchemaDao(client, indexName);
        Set<String> fields = dao.getFieldNames();
        
        if(!fields.contains(Constants.DOI_FIELD))
        {
            Tuple tuple = new Tuple(Constants.DOI_FIELD, "keyword");
            dao.updateSchema(Arrays.asList(tuple));
        }
    }

    
    private void processFile(File file)
    {
        
    }
    
    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: supplementer add-doi <options>");

        System.out.println();
        System.out.println("Add DOI to already registered products.");
        
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>     Path to Sqlite database");
        
        System.out.println();        
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Authentication config file");
        System.out.println("  -es <url>        Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

    
}
