package tt;

import java.util.Map;
import java.util.Set;

import gov.nasa.pds.supp.dao.doi.SqliteReader;


public class TestSqlite
{
    private static class MyCallback implements SqliteReader.Callback
    {
        @Override
        public void onBatch(Map<String, Set<String>> batch)
        {
            System.out.println("===============================");
            batch.forEach((id, dois) -> 
            {
                //if(!id.startsWith("urn:nasa"))
                System.out.println(id + ", " + dois);
            });
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
        SqliteReader reader = new SqliteReader(new MyCallback());
        reader.read("C:/tmp/doi_2.db");
    }
    
}
