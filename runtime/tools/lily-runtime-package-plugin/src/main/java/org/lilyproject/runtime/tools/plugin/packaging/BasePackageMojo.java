/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.tools.plugin.packaging;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.artifact.Artifact;
import org.apache.commons.io.FileUtils;

import java.util.*;
import java.io.File;
import java.io.IOException;

/**
 * Creates a packaged Kauri application from a wiring.xml file.
 *
 * While this is a plugin, it doesn't really work on the current project,
 * but rather gets all its information from the wiring.xml file.
 *
 * @requiresDependencyResolution runtime
 * @description Creates a packaged Kauri application from a wiring.xml file.
 */
public abstract class BasePackageMojo extends AbstractPackageMojo {
    /**
     * Where should the package structure be created.
     *
     * @parameter expression="${project.build.directory}/kauri-package"
     */
    protected String packageDirectory;

    /**
     * @parameter
     */
    protected boolean includeServiceWrapper = true;

    /**
     * @parameter
     */
    protected boolean includeRunInstructions = true;

    /**
     * Custom log4j config file, to be included instead of default.
     *
     * @parameter
     */
    protected File logConfig;

    public void execute() throws MojoExecutionException, MojoFailureException {
        init();
        
        deleteOldPackage();

        getLog().info("Creating packaged Kauri application at " + packageDirectory);

        //
        // Create an artifact repository to include in the webapp, containing:
        //   1. the modules listed in the wiring.xml
        //   2. for each module, the dependencies listed in its classloader.xml
        //
        Set<Artifact> allArtifacts = new HashSet<Artifact>();
        Set<Artifact> moduleArtifacts = getModuleArtifactsFromKauriConfig();
        allArtifacts.addAll(moduleArtifacts);

        for (Artifact moduleArtifact : moduleArtifacts) {
            Set<Artifact> classPathArtifacts = getClassPathArtifacts(moduleArtifact);
            allArtifacts.addAll(classPathArtifacts);
        }

        createRepository(allArtifacts, packageDirectory + "/lib/");

        //
        // Include the configuration data
        //
        getLog().info("Including the configuration.");
        try {
            FileUtils.copyDirectory(new File(confDirectory), new File(packageDirectory + "/conf"), confDirFilter);
        } catch (IOException e) {
            throw new MojoExecutionException("Error while copying configuration directory.", e);
        }

        //
        // Include the service wrapper stuff
        //
        if (includeServiceWrapper) {
            addServiceWrapper();
        }

        //
        // Include logging setup
        //
        addLogSetup();

        //
        //
        //
        if (includeRunInstructions) {
            addRunInstructions();
        }
    }

    private void deleteOldPackage() throws MojoExecutionException {
        getLog().info("Deleting old package, if any.");
        File webappDir = new File(packageDirectory);
        try {
            deleteDirectory(webappDir);
        } catch (IOException e) {
            throw new MojoExecutionException("Unable to delete existing webapp directory at " + packageDirectory, e);
        }
    }

    private void addServiceWrapper() throws MojoExecutionException {
        new File(packageDirectory, "service").mkdirs();

        copyResource("service/service-wrapper.conf", new File(packageDirectory + "/service/service-wrapper.conf"));
        copyResource("service/README.txt", new File(packageDirectory + "/service/README.txt"));

        // unix scripts
        File kauriServiceDest = new File(packageDirectory + "/service/kauri-service");
        copyResource("service/kauri-service", kauriServiceDest);
        setExecutable(kauriServiceDest);

        // windows scripts
        copyResource("service/install-kauri-service.bat", new File(packageDirectory + "/service/install-kauri-service.bat"));
        copyResource("service/uninstall-kauri-service.bat", new File(packageDirectory + "/service/uninstall-kauri-service.bat"));
        copyResource("service/kauri-service.bat", new File(packageDirectory + "/service/kauri-service.bat"));
    }

    private void addLogSetup() throws MojoExecutionException {
        new File(packageDirectory, "logs").mkdir();

        File targetFile = new File(packageDirectory + "/kauri-log4j.properties");

        if (logConfig != null) {
            copyFile(logConfig, targetFile);
        } else {
            copyResource("kauri-log4j.properties", targetFile);
        }
    }

    private void addRunInstructions() throws MojoExecutionException {
        Map<String, String> props = Collections.singletonMap("version.kauri", kauriVersion);
        copyResource("HOW_TO_RUN.txt", new File(packageDirectory + "/HOW_TO_RUN.txt"), props);        
    }
}
