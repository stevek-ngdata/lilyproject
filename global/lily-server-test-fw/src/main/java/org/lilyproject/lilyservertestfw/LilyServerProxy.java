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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.util.zookeeper.ZkConnectException;

public class LilyServerProxy {
    private Log log = LogFactory.getLog(getClass());

    private static Mode MODE;

    private enum Mode { EMBED, CONNECT };
    private static String LILY_MODE_PROP_NAME = "lily.test.mode";

    private LilyServerTestUtility lilyServerTestUtility;

    private String zkConnectString;

    public LilyServerProxy() {
    }

    public void start(String zkConnectString) throws Exception {
        this.zkConnectString = zkConnectString;
        String lilyModeProp = System.getProperty(LILY_MODE_PROP_NAME);
        if (lilyModeProp == null || lilyModeProp.equals("") || lilyModeProp.equals("embed")) {
            MODE = Mode.EMBED;
        } else if (lilyModeProp.equals("connect")) {
            MODE = Mode.CONNECT;
        } else {
            throw new RuntimeException("Unexpected value for " + LILY_MODE_PROP_NAME + ": " + lilyModeProp);
        }
        
        switch (MODE) {
        case EMBED:
            File dir = createTmpConfDir();
            lilyServerTestUtility = new LilyServerTestUtility(dir.getAbsolutePath());
            lilyServerTestUtility.start();
            break;
        case CONNECT:
            break;
        default:
            throw new RuntimeException("Unexpected mode: " + MODE);
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
    
    private File createTmpConfDir() throws URISyntaxException, IOException {
        String suffix = (System.currentTimeMillis() % 100000) + "" + (int)(Math.random() * 100000);
        File dir;
        while (true) {
            String dirName = System.getProperty("java.io.tmpdir") + File.separator + ("lilytest_conf_") + suffix;
            dir = new File(dirName);
            if (dir.exists()) {
                System.out.println("Temporary test directory already exists, trying another location. Currenty tried: " + dirName);
                continue;
            }

            boolean dirCreated = dir.mkdirs();
            if (!dirCreated) {
                throw new RuntimeException("Failed to created temporary test directory at " + dirName);
            }

            break;
        }

        dir.mkdirs();
        dir.deleteOnExit();

        URL confUrl = getClass().getClassLoader().getResource("org/lilyproject/lilyservertestfw/conf/");
        FileUtils.copyDirectory(new File(confUrl.toURI()), dir);
        if (log.isDebugEnabled())
            log.debug("Copied conf resources to tmp dir: " + dir.getAbsolutePath());
        return dir;
    }
}
