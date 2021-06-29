package tt;

import java.util.Arrays;

import gov.nasa.pds.supp.dao.RegistryRequestBuilder;

public class TestRegistryRequestBuilder
{

    public static void main(String[] args) throws Exception
    {
        RegistryRequestBuilder bld = new RegistryRequestBuilder(true);
        String json = bld.createFindVidsByLids(Arrays.asList("lid1", "lid2"), 1000);
        System.out.println(json);
    }

}
