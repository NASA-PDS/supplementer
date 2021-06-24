package gov.nasa.pds.supp.cmd;

import java.io.File;

import gov.nasa.pds.label.Label;

public class SupplementalLabelProcessor
{
    public SupplementalLabelProcessor()
    {
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
                
        label.close();
        
        System.out.println("*********** " + file);
    }
    
}
