package gov.nasa.pds.supp.cmd;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.label.Label;
import gov.nasa.pds.label.object.FieldDescription;
import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.supp.util.Pds2EsDataTypeMap;


/**
 * Process Product_Metadata_Supplemental products.
 * 
 * <p> Processing steps:
 * <ul>
 * <li>Read table and field definitions from File_Area_Metadata section.</li>
 * <li>Update registry schema in Elasticsearch.</li>
 * <li>Update observational products in Elasticsearch registry index with data 
 * from supplemental table by lid / lidvid.</li>
 * </ul>
 * 
 * @author karpenko
 */
public class SupplementalLabelProcessor
{
    private Logger log;
    private Pds2EsDataTypeMap dtMap;
    
    
    public SupplementalLabelProcessor() throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        dtMap = new Pds2EsDataTypeMap();
        dtMap.load(getPds2EsDataTypeCfgFile());
    }

    
    public void process(File file) throws Exception
    {
        if(!file.exists()) throw new Exception("File doesn't exist: " + file);
        
        Label label = Label.open(file);
        if(!"ProductMetadataSupplemental".equals(label.getProductClass().getSimpleName()))
        {
            throw new Exception("Could not process this label. "
                    + "Only 'Product_Metadata_Supplemental' labels are supported: " + file);
        }

        log.info("Processing " + file);
        
        List<TableObject> tables = label.getObjects(TableObject.class);
        if(tables == null || tables.isEmpty())
        {
            log.warn("There are no tables in this file.");
            return;
        }

        // Supplemental products could only have one table
        TableObject table = tables.get(0);
        processTable(table);
        
        label.close();
    }
    
    
    private void processTable(TableObject table) throws Exception
    {
        FieldDescription[] fields = table.getFields();
        if(fields == null || fields.length == 0)
        {
            log.warn("Table has no fields.");
            return;
        }
        
        int lidIndex = 0;
        int lidVidIndex = 0;
        
        for(int i = 0; i < fields.length; i++)
        {
            FieldDescription field = fields[i];
            
            String name = field.getName();
            String type = field.getType().getXMLType();
            
            if("ASCII_LID".equalsIgnoreCase(type))
            {
                lidIndex = i + 1;       // Index starts from 1
            }
            else if("ASCII_LIDVID".equalsIgnoreCase(type))
            {
                lidVidIndex = i + 1;       // Index starts from 1
            }
            else
            {
                String esType = dtMap.getEsDataType(type);
                String esName = toElasticName(name, esType);
                
                System.out.format("%2d  %s\n", i+1, esName);
                
            }
        }
        
        if(lidIndex == 0 && lidVidIndex == 0)
        {
            throw new Exception("Table is missing LID or LIDVID column.");
        }
    }
    
    
    private String toElasticName(String name, String esType)
    {
        name = name.toLowerCase();
        if(name.indexOf(' ') > 0)
        {
            name = name.replaceAll(" ", "_");
        }
        
        return "ops:Supplemental/" + esType + ":" + name;
    }

    
    /**
     * Get default PDS to Elasticsearch data type mapping configuration file.
     * @return File pointing to default configuration file.
     * @throws Exception an exception
     */
    public static File getPds2EsDataTypeCfgFile() throws Exception
    {
        String home = System.getenv("SUPPLEMENTER_HOME");
        if(home == null) 
        {
            throw new Exception("Could not find default configuration directory. " 
                    + "SUPPLEMENTER_HOME environment variable is not set.");
        }

        File file = new File(home, "elastic/data-dic-types.cfg");
        return file;
    }

}
