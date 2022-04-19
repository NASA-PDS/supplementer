package tt;

import java.util.List;

import tt.doi.Record;
import tt.doi.SqliteReader;


public class TestSqlite
{
    private static class MyCallback implements SqliteReader.Callback
    {
        @Override
        public void onBatch(List<Record> batch)
        {
            System.out.println("===============================");
            for(Record rec: batch)
            {
                System.out.println(rec.getId() + ", " + rec.getDois());
            }
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
        SqliteReader reader = new SqliteReader(new MyCallback());
        reader.read("C:/tmp/doi.db");
    }
    
}
