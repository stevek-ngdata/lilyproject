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
import java.util.ArrayList;
import java.util.List;

import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.LilyRuntimeSettings;
import org.lilyproject.runtime.configuration.ConfManager;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.util.MavenUtil;
import org.lilyproject.util.io.Closer;

public class LilyServerTestUtility {
    private LilyRuntime runtime;
    private final String defaultConfDir;
    private final String customConfDir;
    private final File testSpecificConfDir;
    private ArtifactRepository artifactRepository;

    /**
     * LilyServerTestUtility is used to start Lily using the KauriRuntime.
     * 
     * @param defaultConfDir path to the directory containing the default configuration files to startup lily
     * @param customConfDir path to a directory containing custom configuration files which should be used on top
     *                      of the default configuration files
     */
    public LilyServerTestUtility(String defaultConfDir, String customConfDir, File testHome) throws IOException, URISyntaxException {
        this.defaultConfDir = defaultConfDir;
        this.customConfDir = customConfDir;

        // test-specific-conf are changes to the default configuration to optimize for test cases
        testSpecificConfDir = new File(testHome, "test-specific-conf");
    }
    
    public void start() throws Exception {
        // This disable the HBaseConnectionDisposer in Lily which deletes HBase connections on shutdown
        System.setProperty("lily.hbase.deleteConnections", "false");

        LilyRuntimeSettings settings = new LilyRuntimeSettings();
        settings.setRepository(resolveRepository());
        settings.setConfManager(getConfManager());

        runtime = new LilyRuntime(settings);
        runtime.setMode(Mode.getDefault());
        runtime.start();
    }
    
    public void stop() {
        Closer.close(runtime);
    }
    
    private ConfManager getConfManager() {
        List<File> confDirs = new ArrayList<File>();
        confDirs.add(testSpecificConfDir);
        if (customConfDir != null)
            confDirs.add(new File(customConfDir));
        if (defaultConfDir != null)
            confDirs.add(new File(defaultConfDir));
        return new ConfManagerImpl(confDirs);
    }

    private ArtifactRepository resolveRepository() throws IOException {
        if (this.artifactRepository != null) {
            return artifactRepository;
        } else {
            return new Maven2StyleArtifactRepository(MavenUtil.findLocalMavenRepository());
        }
    }

    public ArtifactRepository getArtifactRepository() {
        return artifactRepository;
    }

    public void setArtifactRepository(ArtifactRepository artifactRepository) {
        this.artifactRepository = artifactRepository;
    }

    public LilyRuntime getRuntime() {
        return runtime;
    }
}
