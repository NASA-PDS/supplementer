package gov.nasa.pds.supp.cmd.doi;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.supp.dao.DaoManager;
import gov.nasa.pds.supp.dao.doi.DoiDao;
import gov.nasa.pds.supp.dao.doi.SqliteReader;

public class DoiLoader implements SqliteReader.Callback
{
    protected Logger log;
    
    /**
     * Constructor
     */
    public DoiLoader()
    {
        log = LogManager.getLogger(this.getClass());
    }
    
    
    public void load(File dbFile) throws Exception
    {
        SqliteReader reader = new SqliteReader(this);
        String path = dbFile.getAbsolutePath();
        log.info("Loading DOIs from " + path);
        
        reader.read(path);
    }


    @Override
    public void onBatch(Map<String, Set<String>> batch) throws Exception
    {
        DoiDao dao = DaoManager.getInstance().getDoiDao();

        // Get registered product IDs (LIDVIDs) and its DOIs. Key = LIDVID, Value = DOIs
        Map<String, Set<String>> existingIds = dao.getDois(batch.keySet());
        
        Map<String, Set<String>> changedIds = new TreeMap<>();

        // Only update products if DOIs changed (were added)
        for(Map.Entry<String, Set<String>> entry: existingIds.entrySet())            
        {
            Set<String> additionalDois = batch.get(entry.getKey());
            if(additionalDois != null)
            {
                Set<String> newValue = entry.getValue();
                if(newValue.addAll(additionalDois))
                {
                    changedIds.put(entry.getKey(), newValue);
                }
            }
        }
        

        System.out.println("===============================");
        changedIds.forEach((id, dois) -> 
        {
            System.out.println(id + ", " + dois);
        });

    }
}
