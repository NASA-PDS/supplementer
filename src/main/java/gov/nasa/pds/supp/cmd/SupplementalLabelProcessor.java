package gov.nasa.pds.supp.cmd;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.label.Label;
import gov.nasa.pds.label.object.FieldDescription;
import gov.nasa.pds.label.object.TableObject;


public class SupplementalLabelProcessor
{
    private Logger log;
    
    
    public SupplementalLabelProcessor()
    {
        log = LogManager.getLogger(this.getClass());
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
        
        for(int i = 0; i < fields.length; i++)
        {
            FieldDescription field = fields[i];
            System.out.format("%2d  %s (%s)\n", i+1, field.getName(), field.getType());
        }
        
        
        if(lidIndex == 0)
        {
            throw new Exception("Table is missing LID column.");
        }
    }
}
