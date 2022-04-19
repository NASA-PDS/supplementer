package tt.doi;

import java.util.Set;
import java.util.TreeSet;

public class Record
{
    private String id;
    private Set<String> dois;
    
    public Record(String id, String doi)
    {
        this.id = id;
        dois = new TreeSet<>();
        addDoi(doi);
    }
    
    public void addDoi(String doi)
    {
        if(doi == null || doi.isBlank()) return;
        dois.add(doi);
    }
    
    public String getId()
    {
        return id;
    }
    
    public Set<String> getDois()
    {
        return dois;
    }

}
