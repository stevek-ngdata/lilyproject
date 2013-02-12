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
package org.kauriproject.tools.plugin.packaging;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.artifact.MavenMetadataSource;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.resolver.ArtifactResolutionResult;
import org.apache.maven.artifact.resolver.filter.ArtifactFilter;
import org.apache.commons.io.FileUtils;

import java.util.*;
import java.io.*;


/**
 * Creates a Servlet webapp from a wiring.xml file.
 *
 * While this is a plugin, it doesn't really work on the current project,
 * but rather gets all its information from the wiring.xml file.
 *
 * @requiresDependencyResolution runtime
 * @description Creates a Servlet webapp from a wiring.xml file.
 */
public abstract class BaseWebappMojo extends AbstractPackageMojo {

    /**
     * Where should the webapp structure be created.
     *
     * @parameter expression="${project.build.directory}/webapp"
     */
    protected String webappDirectory;

    /**
     * Pointer to a custom web.xml file.
     *
     * @parameter
     */
    protected String webXmlLocation;

    public void execute() throws MojoExecutionException, MojoFailureException {
        init();

        deleteOldWebapp();

        getLog().info("Creating a Servlet webapp for a Kauri application based on " + confDirectory);

        //
        // Put Kauri Runtime libraries and the servlet wrapper in WEB-INF/lib
        //
        createWebInfLib();

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

        createRepository(allArtifacts, webappDirectory + "/WEB-INF/repository/");

        //
        // Include the configuration data
        //
        getLog().info("Including the configuration.");
        try {
            FileUtils.copyDirectory(new File(confDirectory), new File(webappDirectory + "/WEB-INF/confs/1"), confDirFilter);
        } catch (IOException e) {
            throw new MojoExecutionException("Error while copying configuration directory.", e);
        }

        //
        // Include a web.xml file
        //
        createWebXml();
    }

    private void createWebXml() throws MojoExecutionException {
        getLog().info("Creating the web.xml file.");

        File target = new File(webappDirectory + "/WEB-INF/web.xml");
        if (webXmlLocation != null) {
            copyFile(new File(webXmlLocation), target);
        } else {
            copyResource("web.xml", target);
        }
    }

    private void deleteOldWebapp() throws MojoExecutionException {
        getLog().info("Deleting old webapp, if any.");
        File webappDir = new File(webappDirectory);
        try {
            deleteDirectory(webappDir);
        } catch (IOException e) {
            throw new MojoExecutionException("Unable to delete existing webapp directory at " + webappDirectory, e);
        }
    }

    private void createWebInfLib() throws MojoExecutionException {
        getLog().info("Creating the WEB-INF/lib dir.");
        try {
            Artifact pomArtifact = artifactFactory.createBuildArtifact("org.kauriproject", "kauri-runtime-servlet", kauriVersion, "pom");
            MavenProject project = projectBuilder.buildFromRepository(pomArtifact, remoteRepositories,localRepository);
            List dependencies = project.getDependencies();

            Set dependencyArtifacts = MavenMetadataSource.createArtifacts( artifactFactory, dependencies, null, null, null );
            dependencyArtifacts.add(project.getArtifact());

            List listeners = Collections.EMPTY_LIST;

            ArtifactFilter filter = new ArtifactFilter() {
                public boolean include(Artifact artifact) {
                    return (!"test".equals(artifact.getScope()));
                }
            };

            ArtifactResolutionResult result = resolver.resolveTransitively(dependencyArtifacts, pomArtifact,
                    Collections.EMPTY_MAP, localRepository, remoteRepositories, metadataSource, filter, listeners);

            Set<Artifact> artifacts = result.getArtifacts();

            File libDir = new File(webappDirectory + "/WEB-INF/lib/");
            libDir.mkdirs();

            for (Artifact artifact : artifacts) {
                if (!artifact.getArtifactId().equals("servlet-api"))
                    copyFile(artifact.getFile(), new File(libDir, artifact.getArtifactId() + "-" + artifact.getVersion() + ".jar"));
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error while creating the WEB-INF/lib.", e);
        }
    }
}
