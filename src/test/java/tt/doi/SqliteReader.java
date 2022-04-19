package tt.doi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqliteReader
{
    public static interface Callback
    {
        public void onBatch(List<Record> batch);
    }
    
    private static final int COMMIT_SIZE = 20;
    
    private List<Record> batch;
    private Callback callback;
    
    
    public SqliteReader(Callback cb)
    {
        batch = new ArrayList<>();
        this.callback = cb;
    }
    

    public void read(String dbFilePath) throws Exception
    {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        
        String sql = "select doi, identifier from doi where identifier like 'urn:nasa:%' order by identifier";
        
        try
        {
            String url = "jdbc:sqlite:" + dbFilePath;
            con = DriverManager.getConnection(url);

            System.out.println("Connected to " + dbFilePath);
            
            st = con.createStatement();
            rs = st.executeQuery(sql);

            Record rec = null;
            
            while(rs.next())
            {
                String doi = rs.getString(1);
                String id = rs.getString(2);

                if(rec != null && id.equals(rec.getId()))
                {
                    rec.addDoi(doi);
                }
                else
                {
                    addRecord(rec);
                    rec = new Record(id, doi);
                }
            }
            
            addRecord(rec);
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

    
    private void addRecord(Record rec)
    {
        if(rec == null || rec.getDois().isEmpty()) return;
        
        batch.add(rec);
        if(batch.size() % COMMIT_SIZE == 0)
        {
            processBatch();
        }
    }

    
    private void processBatch()
    {
        if(callback != null) callback.onBatch(batch);
        batch.clear();
    }
}
