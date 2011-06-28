package ${package};

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;

public class TestSample {
    private static LilyProxy LILY_PROXY;

    // The below is commented out by default since the LILY_PROXY.start() call can take quite some time.
    // For more details on using the Lily test framework, please see:
    // http://docs.outerthought.org/lily-docs-current/515-lily.html

    /*
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        LILY_PROXY = new LilyProxy();
        LILY_PROXY.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        LILY_PROXY.stop();
    }

    @Test
    public void testOne() throws Exception {
        LilyClient lilyClient = LILY_PROXY.getLilyServerProxy().getClient();
        // Do stuff
    }

    @Test
    public void testTwo() throws Exception {
        LilyClient lilyClient = LILY_PROXY.getLilyServerProxy().getClient();
        // Do stuff
    }
    */

    /**
     * This is here to have at least one test method in this class since the other ones
     * are commented out by default.
     */
    @Test
    public void dummyTest() throws Exception {

    }
}
