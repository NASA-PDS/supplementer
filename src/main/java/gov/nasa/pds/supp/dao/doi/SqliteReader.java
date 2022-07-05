package gov.nasa.pds.supp.dao.doi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads DOI data from Sqlite database
 * @author karpenko
 */
public class SqliteReader
{
    /**
     * Callback interface. For each batch onBatch() method is called. 
     */
    public static interface Callback
    {
        /**
         * This method is called when new batch of DOI records is created.
         * @param batch a batch of DOI records
         */
        public void onBatch(Map<String, Set<String>> batch) throws Exception;
    }
    
    // Batch size. Commit after this number of records
    private static final int COMMIT_SIZE = 20;
    
    private Map<String, Set<String>> batch;
    private Callback callback;
    
    
    /**
     * Constructor
     * @param cb a callback to be called on every batch of DOI records.
     */
    public SqliteReader(Callback cb)
    {
        batch = new HashMap<>();
        this.callback = cb;
    }
    

    /**
     * Read DOI records from Sqlite database. 
     * Callback passed in the constructor will be called for each batch of records.
     * @param dbFilePath path to Sqlite database
     * @throws Exception an exception
     */
    public void read(String dbFilePath) throws Exception
    {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        
        String sql = "select doi, identifier from doi where identifier like 'urn:%' and is_latest = 1";
        
        try
        {
            String url = "jdbc:sqlite:" + dbFilePath;
            con = DriverManager.getConnection(url);

            st = con.createStatement();
            rs = st.executeQuery(sql);

            while(rs.next())
            {
                String doi = rs.getString(1);
                String id = rs.getString(2);

                if(id == null || doi == null || id.isBlank() || doi.isBlank()) continue;
                
                // Handle duplicate ids
                Set<String> dois = batch.get(id);                
                // ID already exists
                if(dois != null) 
                {
                    dois.add(doi);
                }
                // New ID
                else
                {
                    dois = new TreeSet<>();
                    dois.add(doi);
                    batch.put(id, dois);
                }
                
                // Process this batch
                if(batch.size() % COMMIT_SIZE == 0)
                {
                    processBatch();
                }
            }
            
            // Process any remaining records
            processBatch();
        }
        finally
        {
            close(rs);
            close(st);
            close(con);
        }
    }


    private static void close(AutoCloseable obj)
    {
        if(obj == null) return;
        
        try
        {
            obj.close();
        }
        catch(Exception ex)
        {
            // Ignore
        }
    }

    
    private void processBatch() throws Exception
    {
        if(callback != null) callback.onBatch(batch);
        batch.clear();
    }
}
