/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.lilyservertestfw;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.util.test.TestHomeUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;

public class LilyServerProxy {
    private Mode mode;

    public enum Mode { EMBED, CONNECT }
    private static String LILY_MODE_PROP_NAME = "lily.lilyserverproxy.mode";

    private LilyServerTestUtility lilyServerTestUtility;

    private String zkConnectString;

    private File testHome;

    public LilyServerProxy() throws IOException {
        this(null, null);
    }

    /**
     *
     * @param testDir storage dir to use in case of embedded mode (a subdir will be made)
     */
    public LilyServerProxy(Mode mode, File testDir) throws IOException {
        if (mode == null) {
            String lilyModeProp = System.getProperty(LILY_MODE_PROP_NAME);
            if (lilyModeProp == null || lilyModeProp.equals("") || lilyModeProp.equals("embed")) {
                this.mode = Mode.EMBED;
            } else if (lilyModeProp.equals("connect")) {
                this.mode = Mode.CONNECT;
            } else {
                throw new RuntimeException("Unexpected value for " + LILY_MODE_PROP_NAME + ": " + lilyModeProp);
            }
        } else {
            this.mode = mode;
        }

        if (this.mode == Mode.EMBED) {
            if (testDir == null) {
                testHome = TestHomeUtil.createTestHome("lilyserverproxy-");
            } else {
                testHome = new File(testDir, "lilyserverproxy");
                FileUtils.forceMkdir(testHome);
            }
        }
    }

    public void start(String zkConnectString) throws Exception {
        System.out.println("LilyServerProxy mode: " + mode);

        this.zkConnectString = zkConnectString;

        switch (mode) {
            case EMBED:
                FileUtils.forceMkdir(testHome);
                FileUtils.cleanDirectory(testHome);
                System.out.println("LilySeverProxy embedded mode temp dir: " + testHome.getAbsolutePath());
                File confDir = new File(testHome, "conf");
                FileUtils.forceMkdir(confDir);
                extractTemplateConf(confDir);
                lilyServerTestUtility = new LilyServerTestUtility(confDir.getAbsolutePath());
                lilyServerTestUtility.start();
                break;
            case CONNECT:
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + mode);
        }
    }
    
    public void stop() {
        if (lilyServerTestUtility != null)
            lilyServerTestUtility.stop();
    }
    
    public LilyClient getClient() throws IOException, InterruptedException, KeeperException, ZkConnectException, NoServersException {
        if (zkConnectString != null)
            return new LilyClient(zkConnectString, 10000);
        else 
            return new LilyClient("localhost:2181", 10000);
    }
    
    private void extractTemplateConf(File confDir) throws URISyntaxException, IOException {
        URL confUrl = getClass().getClassLoader().getResource(ConfUtil.CONF_RESOURCE_PATH);
        ConfUtil.copyConfResources(confUrl, ConfUtil.CONF_RESOURCE_PATH, confDir);
    }
}
