package tt;

import java.io.File;

import gov.nasa.pds.label.Label;
import gov.nasa.pds.label.object.FieldDescription;
import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.label.object.TableRecord;

public class JParserTest1
{

    public static void main(String[] args) throws Exception
    {
        Label label = Label.open(new File("/ws3/Cassini/vims/raw/index.xml"));
        
        //ProductMetadataSupplemental sup = (ProductMetadataSupplemental)label.genericProduct;
        
        
        TableObject table = label.getObjects(TableObject.class).get(0);
        TableRecord rec = table.readNext();
        
        FieldDescription[] fields = table.getFields();
        for(int i = 0; i < fields.length; i++)
        {
            FieldDescription field = fields[i];
            System.out.format("%2d  %s (%s)  ->  %s\n", 
                i+1, field.getName(), field.getType(), rec.getString(i+1));
        }
    }

}
