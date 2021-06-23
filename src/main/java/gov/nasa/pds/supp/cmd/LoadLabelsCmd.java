package gov.nasa.pds.supp.cmd;

import org.apache.commons.cli.CommandLine;

/**
 * Load supplemental data from Product_Metadata_Supplemental labels into registry index 
 * @author karpenko
 */
public class LoadLabelsCmd implements CliCommand
{
    
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

}
