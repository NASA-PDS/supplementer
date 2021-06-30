package tt;

import java.io.File;
import java.util.List;

import gov.nasa.pds.label.Label;
import gov.nasa.pds.label.object.FieldDescription;
import gov.nasa.pds.label.object.TableObject;
import gov.nasa.pds.label.object.TableRecord;

public class JParserTest1
{

    public static void main(String[] args) throws Exception
    {
        Label label = Label.open(new File("/ws3/Cassini/vims/raw/index.xml"));
        
        List<TableObject> tables = label.getObjects(TableObject.class);
        TableObject table = tables.get(0);
        
        TableRecord rec = table.readNext();
        
        FieldDescription[] fields = table.getFields();
        for(int i = 0; i < fields.length; i++)
        {
            FieldDescription field = fields[i];
            System.out.format("%2d  %s (%s)  ->  %s\n", 
                i+1, field.getName(), field.getType(), rec.getString(i+1));
        }
        
        label.close();
    }

}
