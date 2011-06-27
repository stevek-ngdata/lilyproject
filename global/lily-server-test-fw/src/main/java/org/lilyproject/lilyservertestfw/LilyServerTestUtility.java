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
import java.util.ArrayList;
import java.util.List;

import org.kauriproject.runtime.KauriRuntime;
import org.kauriproject.runtime.KauriRuntimeSettings;
import org.kauriproject.runtime.configuration.ConfManager;
import org.kauriproject.runtime.configuration.ConfManagerImpl;
import org.kauriproject.runtime.rapi.Mode;
import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.util.MavenUtil;
import org.lilyproject.util.io.Closer;

public class LilyServerTestUtility {

    private final String confDir;
    private KauriRuntime runtime;

    public LilyServerTestUtility(String confDir) {
        this.confDir = confDir;
    }
    
    public void start() throws Exception {
        // This disable the HBaseConnectionDisposer in Lily which deletes HBase connections on shutdown
        System.setProperty("lily.hbase.deleteConnections", "false");

        KauriRuntimeSettings settings = new KauriRuntimeSettings();
        settings.setRepository(getRepository());
        settings.setConfManager(getConfManager());

        runtime = new KauriRuntime(settings);
        runtime.setMode(Mode.getDefault());
        runtime.start();
    }
    
    public void stop() {
        Closer.close(runtime);
    }
    
    private ConfManager getConfManager() {
        List<File> confDirs = new ArrayList<File>();
        if (confDir != null) 
            confDirs.add(new File(confDir));
        return new ConfManagerImpl(confDirs);
    }

    private ArtifactRepository getRepository() throws IOException {
        return new Maven2StyleArtifactRepository(MavenUtil.findLocalMavenRepository());
    }

    public KauriRuntime getRuntime() {
        return runtime;
    }
}
