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

import java.io.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.KeeperException;
import org.kauriproject.runtime.KauriRuntime;
import org.kauriproject.runtime.KauriRuntimeSettings;
import org.kauriproject.runtime.configuration.ConfManager;
import org.kauriproject.runtime.configuration.ConfManagerImpl;
import org.kauriproject.runtime.rapi.Mode;
import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.client.NoServersException;
import org.lilyproject.indexer.model.api.*;
import org.lilyproject.indexer.model.impl.IndexerModelImpl;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.util.zookeeper.ZkConnectException;

public class LilyServerTestUtility {

    private final String confDir;
    private KauriRuntime runtime;
    private File tmpDir;

    public LilyServerTestUtility(String confDir) {
        this.confDir = confDir;
        tmpDir = createTempDir();
    }
    
    public void start() throws Exception {
        KauriRuntimeSettings settings = new KauriRuntimeSettings();
        settings.setRepository(getRepository());
        settings.setConfManager(getConfManager());

        runtime = new KauriRuntime(settings);
        runtime.setMode(Mode.getDefault());
        runtime.start();
    }
    
    public void stop() {
        if (runtime != null) {
            runtime.stop();
        }

        if (tmpDir != null) {
            try {
                FileUtils.deleteDirectory(tmpDir);
            } catch (IOException e) {
                // ignore
            }
        }
    }
    
    private ConfManager getConfManager() {
        List<File> confDirs = new ArrayList<File>();
        if (confDir != null) 
            confDirs.add(new File(confDir));
        return new ConfManagerImpl(confDirs);
    }

    
    private ArtifactRepository getRepository() {
        String localRepositoryPath = System.getProperty("localRepository");
        if (localRepositoryPath == null)
            localRepositoryPath = System.getProperty("user.home") + "/.m2/repository";

        return new Maven2StyleArtifactRepository(new File(localRepositoryPath));
    }
    
    private File createTempDir() {
        String suffix = (System.currentTimeMillis() % 100000) + "" + (int)(Math.random() * 100000);
        File dir;
        while (true) {
            String dirName = System.getProperty("java.io.tmpdir") + File.separator + ("lilytest_") + suffix;
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

        return dir;
    }
}
