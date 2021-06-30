package gov.nasa.pds.supp.cmd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.supp.dao.DaoManager;

/**
 * Load supplemental data from Product_Metadata_Supplemental labels into registry index 
 * @author karpenko
 */
public class LoadLabelsCmd implements CliCommand
{
    private SupplementalLabelProcessor proc;
    
    
    /**
     * Constructor
     */
    public LoadLabelsCmd()
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

        RestClient client = null;
        
        try
        {
            // Init Elasticsearch client and DAOs
            client = EsClientFactory.createRestClient(esUrl, authPath);
            DaoManager.init(client, indexName);
            
            // Process supplemental (list) file
            proc = new SupplementalLabelProcessor();
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
        System.out.println("Usage: supplementer load-labels <options>");

        System.out.println();
        System.out.println("Load supplemental data from Product_Metadata_Supplemental labels into registry index");
        
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>     Either Product_Metadata_Supplemental label file (.xml)");
        System.out.println("                   or a text file (.txt) with the list of label files"); 
        System.out.println("                   (one file path per line).");
        
        System.out.println();        
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Authentication config file");
        System.out.println("  -es <url>        Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Elasticsearch index name. Default is 'registry'");

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
