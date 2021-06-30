package gov.nasa.pds.supp;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Supplementer main class.
 * 
 * @author karpenko
 */
public class SuppMain
{
    public static void main(String[] args)
    {
        // We don't use "java.util" logger.
        Logger log = Logger.getLogger("");
        log.setLevel(Level.OFF);

        SuppCli cli = new SuppCli();
        cli.run(args);
    }

}
