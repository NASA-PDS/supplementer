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
        
        FieldDescription[] fields = table.getFields();
        FieldDescription field = fields[0];
        
        System.out.println(field.getType());
        
        TableRecord rec = table.readNext();
        System.out.println(rec.getString(1));
    }

}
