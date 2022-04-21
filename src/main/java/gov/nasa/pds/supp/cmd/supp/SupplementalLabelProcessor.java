package gov.nasa.pds.supp.cmd.supp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.label.Label;
import gov.nasa.pds.label.object.FieldDescription;
import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.supp.dao.DaoManager;
import gov.nasa.pds.supp.dao.SchemaDao;
import gov.nasa.pds.supp.util.Pds2EsDataTypeMap;
import gov.nasa.pds.supp.util.Tuple;


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
    private SupplementalDataLoader loader;
    
    
    public SupplementalLabelProcessor() throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        dtMap = new Pds2EsDataTypeMap();
        dtMap.load(getPds2EsDataTypeCfgFile());
        
        loader = new SupplementalDataLoader(); 
    }

    
    /**
     * Process supplemental product PDS4 label (XML file)
     * @param file XML file (Product_Metadata_Supplemental product)
     * @throws Exception an exception
     */
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
    
    
    /**
     * Process data table of the supplemental product 
     * @param table supplemental data table
     * @throws Exception an exception
     */
    private void processTable(TableObject table) throws Exception
    {
        // Validate table
        FieldDescription[] fields = table.getFields();
        if(fields == null || fields.length == 0)
        {
            log.warn("Table has no fields.");
            return;
        }
        
        // Get Elasticsearch field info 
        SupplementalFieldsInfo esFieldInfo = getElasticFieldInfo(fields);
        
        // LID or LIDVID field is required
        if(esFieldInfo.lidIndex == 0 && esFieldInfo.lidVidIndex == 0)
        {
            throw new Exception("Table is missing LID or LIDVID column.");
        }

        // Update Elasticsearch schema
        updateSchema(esFieldInfo);
        
        // Load data
        loader.loadData(table, esFieldInfo);
    }
    
    
    private void updateSchema(SupplementalFieldsInfo info) throws Exception
    {
        SchemaDao dao = DaoManager.getInstance().getSchemaDao();
        
        // Get list of existing supplemental fields from Elasticsearch
        Set<String> existingFields = dao.getSupplementalFieldNames();
        
        List<Tuple> newFields = new ArrayList<>();
        
        // Iterate over supplemental table columns
        // Index starts from 1
        for(int i = 1; i <= info.size(); i++)
        {
            String name = info.getName(i);
            
            // LID or LIDVID field
            if(name == null) continue;
            // Field already exists in Elasticsearch
            if(existingFields.contains(name)) continue;
            
            // Add non-existing field name and type to the new field list
            Tuple tuple = new Tuple(name, info.getDataType(i));
            newFields.add(tuple);
        }
        
        // Update Elasticsearch schema
        if(!newFields.isEmpty())
        {
            log.info("Updating Elasticsearch schema.");
            dao.updateSchema(newFields);
        }
    }
    
    
    private SupplementalFieldsInfo getElasticFieldInfo(FieldDescription[] fields)
    {
        SupplementalFieldsInfo info = new SupplementalFieldsInfo(fields.length);
        
        for(int i = 0; i < fields.length; i++)
        {
            FieldDescription field = fields[i];
            
            String name = field.getName();
            String type = field.getType().getXMLType();
            
            if("ASCII_LID".equalsIgnoreCase(type))
            {
                info.lidIndex = i + 1;       // Index starts from 1
            }
            else if("ASCII_LIDVID".equalsIgnoreCase(type))
            {
                info.lidVidIndex = i + 1;       // Index starts from 1
            }
            else
            {
                String esType = dtMap.getEsDataType(type);
                String esName = toElasticName(name, esType);
                // Index starts from 1
                info.setFieldInfo(i + 1, esName, esType);
            }
        }
        
        return info;
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
    private static File getPds2EsDataTypeCfgFile() throws Exception
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
