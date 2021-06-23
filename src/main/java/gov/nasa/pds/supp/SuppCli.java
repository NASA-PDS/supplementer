package gov.nasa.pds.supp;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.supp.cmd.CliCommand;
import gov.nasa.pds.supp.cmd.LoadLabelsCmd;
import gov.nasa.pds.supp.util.ExceptionUtils;
import gov.nasa.pds.supp.util.log.Log4jConfigurator;

/**
 * Supplementer Command-Line Interface (CLI) manager / command runner.
 * 
 * @author karpenko
 */
public class SuppCli
{
    private Logger log;
    
    private Options options;
    private CommandLine cmdLine;
    
    private Map<String, CliCommand> commands;
    private CliCommand command;
    
    
    /**
     * Constructor
     */
    public SuppCli()
    {
        initCommands();
        initOptions();
    }
    

    /**
     * Parse command line arguments and run commands.
     * @param args command line arguments passed from the main() function.
     */
    public void run(String[] args)
    {
        if(args.length == 0)
        {
            printHelp();
            System.exit(1);
        }

        if(!parse(args))
        {
            System.out.println();
            printHelp();
            System.exit(1);
        }

        initLogger();
        log = LogManager.getLogger(this.getClass());
        
        if(!runCommand())
        {
            System.exit(1);
        }        
    }
    

    /**
     * Run commands based on command line parameters.
     * @return
     */
    private boolean runCommand()
    {
        try
        {
            command.run(cmdLine);
            return true;
        }
        catch(Exception ex)
        {
            log.error(ExceptionUtils.getMessage(ex));
            return false;
        }
    }

    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: supplementer <command> <options>");

        System.out.println();
        System.out.println("Commands:");
        System.out.println("  load-labels   Load supplemental data from Product_Metadata_Supplemental labels");
        System.out.println("                into registry index");
        
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -help         Print help for a command");
        System.out.println("  -l <file>     Log file. Default is /tmp/supplementer/supplementer.log");
        System.out.println("  -v <level>    Logger verbosity: DEBUG, INFO (default), WARN, ERROR");
        
        System.out.println();
        System.out.println("Pass -help after any command to see command-specific usage information, for example,");
        System.out.println("  supplementer load-labels -help");
    }

    
    /**
     * Parse command line parameters
     * @param args
     * @return
     */
    private boolean parse(String[] pArgs)
    {
        try
        {
            CommandLineParser parser = new DefaultParser();
            this.cmdLine = parser.parse(options, pArgs);
            
            String[] args = cmdLine.getArgs();
            if(args == null || args.length == 0)
            {
                System.out.println("[ERROR] Missing command.");
                return false;
            }

            if(args.length > 1)
            {
                System.out.println("[ERROR] Invalid command: " + String.join(" ", args)); 
                return false;
            }
            
            this.command = commands.get(args[0]);
            if(this.command == null)
            {
                System.out.println("[ERROR] Invalid command: " + args[0]);
                return false;
            }
            
            return true;
        }
        catch(ParseException ex)
        {
            System.out.println("[ERROR] " + ex.getMessage());
            return false;
        }
    }
    
    
    /**
     * Initialize Log4j logger
     */
    private void initLogger()
    {
        String verbosity = cmdLine.getOptionValue("v", "INFO");
        String logFile = cmdLine.getOptionValue("l");

        Log4jConfigurator.configure(verbosity, logFile);
    }

    
    /**
     * Initialize all CLI commands
     */
    private void initCommands()
    {
        commands = new HashMap<>();
        commands.put("load-labels", new LoadLabelsCmd());
    }
    
    
    /**
     * Initialize Apache Commons CLI library
     */
    private void initOptions()
    {
        options = new Options();

        Option.Builder bld;

        // Common
        bld = Option.builder("help");
        options.addOption(bld.build());
        
        bld = Option.builder("l").hasArg().argName("file");
        options.addOption(bld.build());

        bld = Option.builder("v").hasArg().argName("level");
        options.addOption(bld.build());

        // File
        bld = Option.builder("file").hasArg().argName("path");
        options.addOption(bld.build());

        // Registry (Elasticsearch)
        bld = Option.builder("auth").hasArg().argName("file");
        options.addOption(bld.build());

        bld = Option.builder("es").hasArg().argName("url");
        options.addOption(bld.build());

        bld = Option.builder("index").hasArg().argName("name");
        options.addOption(bld.build());

    }

}
