package gov.nasa.pds.supp.cmd.supp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.cmd.CliCommand;
import gov.nasa.pds.supp.dao.DaoManager;

/**
 * CLI command to update products already stored in Elasticsearch registry index  
 * with supplemental data from Product_Metadata_Supplemental labels. 
 *  
 * @author karpenko
 */
public class AddSupplementalFieldsCmd implements CliCommand
{
    private SupplementalLabelProcessor proc;
    
    
    /**
     * Constructor
     */
    public AddSupplementalFieldsCmd()
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

        String lowerCaseName = pFile.toLowerCase();
        if(!lowerCaseName.endsWith(".xml") && !lowerCaseName.endsWith(".txt")) 
        {
            throw new Exception("Unknown file type. Only '.xml' or '.txt' files are allowed: " + pFile);            
        }

        // Init label processor
        proc = new SupplementalLabelProcessor();

        // Init Elasticsearch client and DAOs
        RestClient client = null;
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);
            DaoManager.init(client, indexName);
            
            // Process supplemental (list) file
            processFile(pFile);
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

    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: supplementer add-supplemental-fields <options>");

        System.out.println();
        System.out.println("Add supplemental fields to already registered products.");
        
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>     Either Product_Metadata_Supplemental label file (.xml) or a");
        System.out.println("                   text manifest file (.txt) with the list of supplemental label paths"); 
        System.out.println("                   (one file path per line).");
        
        System.out.println();        
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Registry authentication configuration file");
        System.out.println("  -es <url>        Registry (OpenSearch) URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Registry index name. Default is 'registry'");

        System.out.println();
    }

    
    private void processFile(String filePath) throws Exception
    {
        String lowerCaseName = filePath.toLowerCase();
        if(lowerCaseName.endsWith(".xml"))
        {
            processSupplementalLabel(filePath);
        }
        else if(lowerCaseName.endsWith(".txt"))
        {
            processLabelListFile(filePath);
        }
    }
    

    private void processSupplementalLabel(String filePath) throws Exception
    {
        proc.process(new File(filePath));
    }

    
    private void processLabelListFile(String filePath) throws Exception
    {
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(filePath));
            
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                
                if(line.length() > 0)
                {
                    // Skip comments
                    if(line.startsWith("#")) continue;

                    processSupplementalLabel(line);
                }
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }

}
