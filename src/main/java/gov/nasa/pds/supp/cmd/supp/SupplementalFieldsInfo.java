package gov.nasa.pds.supp.cmd.supp;

/**
 * Elasticsearch name and data type of supplemental fields.
 * 
 * @author karpenko
 */
public class SupplementalFieldsInfo
{
    private int numFields;
    private String[] fieldNames;
    private String[] fieldTypes;

    public int lidIndex;
    public int lidVidIndex;

    
    /**
     * Constructor
     * @param numFields number of fields
     */
    public SupplementalFieldsInfo(int numFields)
    {
        this.numFields = numFields;
        fieldNames = new String[numFields];
        fieldTypes = new String[numFields];
    }
    
    
    /**
     * Get number of fields.
     * @return number of fields
     */
    public int size()
    {
        return numFields;
    }
    
    
    /**
     * Get Elasticsearch field name by supplemental table column index.
     * Note, index starts from 1.
     * @param index Field / column index. Note, index starts from 1.
     * @return Elasticsearch field name (table column name)
     */
    public String getName(int index)
    {
        return fieldNames[index-1];
    }
    

    /**
     * Get Elasticsearch data type by supplemental table column index.
     * Note, index starts from 1.
     * @param index Field / column index. Note, index starts from 1.
     * @return Elasticsearch data type
     */
    public String getDataType(int index)
    {
        return fieldTypes[index-1];
    }
    
    
    /**
     * Set Elasticsearch field name and data type by supplemental table column index.
     * Note, index starts from 1.
     * @param index Field / column index. Note, index starts from 1.
     * @param name Elasticsearch field name
     * @param dataType Elasticsearch data type
     */
    public void setFieldInfo(int index, String name, String dataType)
    {
        fieldNames[index-1] = name;
        fieldTypes[index-1] = dataType;
    }

}
