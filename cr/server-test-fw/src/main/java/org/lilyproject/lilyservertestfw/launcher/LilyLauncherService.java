/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.lilyservertestfw.launcher;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.lilyproject.runtime.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.runtime.repository.ChainedMaven2StyleArtifactRepository;
import org.lilyproject.lilyservertestfw.ConfUtil;
import org.lilyproject.lilyservertestfw.LilyServerTestUtility;
import org.lilyproject.lilyservertestfw.TemplateDir;
import org.lilyproject.util.MavenUtil;

public class LilyLauncherService implements LauncherService {
    private LilyServerTestUtility lilyServerTestUtility;
    private File testHome;
    private File userConfDir = null;
    private File defaultConfDir = null;
    private ArtifactRepository artifactRepository;

    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome, boolean clearData) throws Exception {
        this.testHome = new File(testHome, TemplateDir.LILYSERVER_DIR);
        String confDirPath = System.getProperty("lily.conf.dir");
        if (confDirPath != null) {
            userConfDir = new File(confDirPath);
        }

        if (!userConfDir.exists()) {
            System.err.println("Lily conf dir does not exist: " + userConfDir);
            return 1;
        }

        defaultConfDir = new File(testHome, "lilyconf");
        FileUtils.forceMkdir(defaultConfDir);
        URL confUrl = getClass().getClassLoader().getResource(ConfUtil.CONF_RESOURCE_PATH);
        ConfUtil.copyConfResources(confUrl, ConfUtil.CONF_RESOURCE_PATH, defaultConfDir);

        String repository = System.getProperty("lily.testlauncher.repository");
        if (repository != null) {
            // Since this is for development, we always add the local Maven repository
            repository = MavenUtil.findLocalMavenRepository() + "," + repository;
            artifactRepository = new ChainedMaven2StyleArtifactRepository(repository);
        }

        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        lilyServerTestUtility = new LilyServerTestUtility(defaultConfDir.getAbsolutePath(),
                userConfDir.getAbsolutePath(), testHome);
        lilyServerTestUtility.setArtifactRepository(artifactRepository);
        lilyServerTestUtility.start();

        postStartupInfo.add("-----------------------------------------------");
        postStartupInfo.add("Lily is running");
        postStartupInfo.add("");
        postStartupInfo.add("Using configuration from: " + defaultConfDir.getAbsolutePath());
        postStartupInfo.add("You can connect a LilyClient to it using zookeeper connect string \"localhost:2181\"");
        postStartupInfo.add("");
        postStartupInfo.add("REST interface available at: http://localhost:12060/");
        postStartupInfo.add("");
        postStartupInfo.add("From Java, use:");
        postStartupInfo.add("LilyClient lilyClient = new LilyClient(\"localhost:2181\", 20000);");
        postStartupInfo.add("");

        return 0;
    }

    @Override
    public void stop() {
        if (lilyServerTestUtility != null) {
            lilyServerTestUtility.stop();
            lilyServerTestUtility = null;
        }
    }
}
