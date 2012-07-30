package org.lilyproject.solrtestfw.test;

import org.junit.Test;
import org.lilyproject.solrtestfw.SolrDefinition;
import org.lilyproject.solrtestfw.SolrProxy;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.fail;

public class SolrProxyTest {
    @Test
    public void testDynamicallyChangingNumberOfCores() throws Exception {
        SolrProxy solr = new SolrProxy();
        solr.start();

        testSolrCore("core0", 200);
        testSolrCore("core1", 404);

        // Increase number of cores
        solr.changeSolrDefinition(new SolrDefinition(
                SolrDefinition.core("core0"),
                SolrDefinition.core("core1")
        ));

        testSolrCore("core0", 200);
        testSolrCore("core1", 200);
        testSolrCore("core2", 404);

        // Reduce number of cores
        solr.changeSolrDefinition(new SolrDefinition(
                SolrDefinition.core("core0")
        ));

        testSolrCore("core0", 200);
        testSolrCore("core1", 404);

        // Increase number of cores again
        solr.changeSolrDefinition(new SolrDefinition(
                SolrDefinition.core("core0"),
                SolrDefinition.core("core1"),
                SolrDefinition.core("core2")
        ));

        testSolrCore("core0", 200);
        testSolrCore("core1", 200);
        testSolrCore("core2", 200);
        testSolrCore("core3", 404);

        solr.stop();
    }

    @Test
    public void testNoCore0() throws Exception {
        SolrProxy solr = new SolrProxy();
        solr.start();

        solr.changeSolrDefinition(new SolrDefinition(
                SolrDefinition.core("first"),
                SolrDefinition.core("second")
        ));

        // Even when not specified, there is always the core0 core
        testSolrCore("core0", 200);
        testSolrCore("first", 200);
        testSolrCore("second", 200);

        testDefaultSolrCore(200);

        solr.stop();
    }

    private void testSolrCore(String coreName, int expectedStatus) throws IOException {
        String urlString = "http://localhost:8983/solr/" + coreName + "/select?q=*:*";
        HttpURLConnection conn = performRequest(urlString);
        if (conn.getResponseCode() != expectedStatus) {
            fail("Testing core " + coreName + ": expected status " + expectedStatus +
                    " but got: " + conn.getResponseCode() + ": " + conn.getResponseMessage());
        }
    }

    private void testDefaultSolrCore(int expectedStatus) throws IOException {
        String urlString = "http://localhost:8983/solr/select?q=*:*";
        HttpURLConnection conn = performRequest(urlString);
        if (conn.getResponseCode() != expectedStatus) {
            fail("Testing default core: expected status " + expectedStatus +
                    " but got: " + conn.getResponseCode() + ": " + conn.getResponseMessage());
        }
    }

    private HttpURLConnection performRequest(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.connect();
        conn.disconnect();
        return conn;
    }

}
