package org.lilyproject.container.jetty;

import static java.lang.System.setProperty;
import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import com.sun.jersey.api.client.Client;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.container.jetty.CustomJettyLauncher;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.runtime.module.javaservice.JavaServiceManager;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.util.test.TestHomeUtil;
import org.springframework.core.io.ClassPathResource;


public class JettyIntegrationTest {

    private static LilyProxy proxy;
    private static final String[] confLoadBeforeWiringFiles = {};
    private static final String[] confLoadAtEndWiringFiles = {"/plugins/load-at-end/lily-jetty-test.xml"};
    private static File tmpDir;

    @BeforeClass
    public static void setup() throws Exception{
        configureLily();
        startLily();
    }


    public static void configureLily() throws Exception{
        tmpDir = TestHomeUtil.createTestHome("lily-jetty-container-test-");
        File customConfDir = setupConfDirectory(tmpDir);
        File customSetupDir = setupPluginsDirectory(tmpDir);
        setupSSLkeystore(tmpDir);
        setProperty("lily.plugin.dir", customSetupDir.getAbsolutePath());
        setProperty("lily.conf.customdir", customConfDir.getAbsolutePath());
    }


    public static void startLily() throws Exception {
        proxy = new LilyProxy();
        proxy.start();
    }

    @AfterClass()
    public static void cleanUp() throws Exception{
        try{
            proxy.stop();
        } finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    protected static void setupSSLkeystore(File tmpDir) throws IOException {
        ClassPathResource resource = new ClassPathResource("/ssl/keystore");
        File keystore = new File(tmpDir, "keystore");
        FileUtils.copyInputStreamToFile(resource.getInputStream(), keystore);
        System.setProperty(CustomJettyLauncher.LILY_SSL_KEYSTORE, keystore.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStore", keystore.getAbsolutePath());
    }

    protected static File setupConfDirectory(File tmpDir) throws Exception{
        File confDir = new File(tmpDir, "conf");
        File lilyConfDir = new File(confDir, "lily");
        FileUtils.deleteDirectory(lilyConfDir);
        FileUtils.forceMkdir(lilyConfDir);

        setupSingleConfigFile(confDir, "jetty", "jetty", "/conf/jetty-with-ssl.xml");
        return confDir;
    }

    protected static File setupPluginsDirectory(File tmpDir) throws Exception {
        File pluginDir = new File(tmpDir, "plugins");
        FileUtils.deleteDirectory(pluginDir);
        FileUtils.forceMkdir(pluginDir);
        setupSinglePluginDirectory(pluginDir, "load-before-repository", confLoadBeforeWiringFiles);
        setupSinglePluginDirectory(pluginDir, "load-at-end", confLoadAtEndWiringFiles);
        return pluginDir;
    }

    private static void setupSinglePluginDirectory(File pluginDir, String pluginSubDir, String[] xmlFilePaths)
            throws Exception {
        File pDir = new File(pluginDir, pluginSubDir);
        FileUtils.forceMkdir(pDir);
        for (String xmlFilePath : xmlFilePaths) {
            ClassPathResource resource = new ClassPathResource(xmlFilePath);
            String fileName = new File(xmlFilePath).getName();
            FileUtils.copyInputStreamToFile(resource.getInputStream() ,new File(pDir, fileName));
        }
    }

    private static void setupSingleConfigFile(File configDir, String moduleId, String configName, String resourceName)
            throws IOException {
        File moduleDir = new File(configDir, moduleId);
        moduleDir.mkdirs();
        ClassPathResource resource = new ClassPathResource(resourceName);
        FileUtils.copyInputStreamToFile(resource.getInputStream() ,new File(moduleDir, configName + ".xml"));
    }

    @Test
    public void testFilterIsLoaded() throws Exception {
        JavaServiceManager serviceManager =
                proxy.getLilyServerProxy().getLilyServerTestingUtility().getRuntime().getJavaServiceManager();
        ServletRegistry service = (ServletRegistry) serviceManager.getService(ServletRegistry.class);
        assertThat(service.getFilterEntries()).hasSize(1);
    }

    @Test
    public void testResApiIsReady() throws Exception {
        Client client = new Client();
        JSONObject object = client.resource("http://localhost:12060/repository/schema/recordType").get(JSONObject.class);
        assertThat(object).isNotNull();
        assertThat(object.keys()).contains("results");
    }

    @Test
    public void testHttpsIsReady() throws Exception {
        Client client = new Client();
        JSONObject object = client.resource("https://localhost:12443/repository/schema/recordType").get(JSONObject.class);
        assertThat(object).isNotNull();
        assertThat(object.keys()).contains("results");
    }
}
