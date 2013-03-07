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
package org.lilyproject.tools.mavenplugin.lilyruntimedepresolver;

import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProjectBuilder;
import org.lilyproject.runtime.model.SourceLocations;

/**
 *
 * @goal resolve-project-dependencies
 * @requiresDependencyResolution runtime
 * @description Resolve (download) all the dependencies of a Lily Runtime project starting from wiring.xml.
 */
public class LilyRuntimeProjectDependencyResolver extends AbstractMojo {
    /**
     * Location of the conf directory.
     *
     * @parameter
     */
    protected String confDirectory;

    /**
     * Location of a wiring.xml file on the classpath.
     *
     * @parameter
     */
    protected String wiringXmlResource;

    /**
     * Location of the module-source-locations.properties file.
     *
     * @parameter
     */
    protected String moduleSourceLocations;

    /**
     * @parameter expression="${project.groupId}"
     * @required
     * @readonly
     */
    private String projectGroupId;

    /**
     * @parameter expression="${project.artifactId}"
     * @required
     * @readonly
     */
    private String projectArtifactId;

    /**
     * @parameter expression="${project.version}"
     * @required
     * @readonly
     */
    private String projectVersion;

    /**
     * @parameter expression="${project.dependencyArtifacts}"
     * @required
     * @readonly
     */
    private Set<Artifact> dependencyArtifacts;

    /**
     * Maven Artifact Factory component.
     *
     * @component
     */
    protected ArtifactFactory artifactFactory;

    /**
     * Remote repositories used for the project.
     *
     * @parameter expression="${project.remoteArtifactRepositories}"
     * @required
     * @readonly
     */
    protected List remoteRepositories;

    /**
     * Local Repository.
     *
     * @parameter expression="${localRepository}"
     * @required
     * @readonly
     */
    protected ArtifactRepository localRepository;

    /**
     * Artifact Resolver component.
     *
     * @component
     */
    protected ArtifactResolver resolver;

    /**
     * @component role="org.apache.maven.project.MavenProjectBuilder"
     * @required
     * @readonly
     */
    protected MavenProjectBuilder mavenProjectBuilder;


    protected XPathFactory xpathFactory = XPathFactory.newInstance();
    protected SourceLocations sourceLocations = new SourceLocations();

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if ((confDirectory == null && wiringXmlResource == null)
                || (confDirectory != null && wiringXmlResource != null)) {
            throw new MojoExecutionException("Either confDirectory or wiringXmlResource should be specified.");
        }

        if (moduleSourceLocations != null) {
            File sourceLocationsFile = new File(moduleSourceLocations);
            FileInputStream sourceLocationsStream = null;
            try {
                sourceLocationsStream = new FileInputStream(sourceLocationsFile);
                sourceLocations = new SourceLocations(sourceLocationsStream, sourceLocationsFile.getParentFile().getAbsolutePath());
            } catch (Exception e) {
                throw new MojoExecutionException("Problem reading module source locations file from " +
                        sourceLocationsFile.getAbsolutePath(), e);
            } finally {
                if (sourceLocationsStream != null) {
                    try { sourceLocationsStream.close(); } catch (IOException e) { e.printStackTrace(); }
                }
            }
        }

        LilyRuntimeProjectClasspath cp = new LilyRuntimeProjectClasspath(getLog(), new MyArtifactFilter(),
                artifactFactory, resolver, localRepository);

        ModuleArtifacts moduleArts;
        if (confDirectory != null) {
            moduleArts = cp.getModuleArtifactsFromLilyRuntimeConfig(new File(confDirectory), remoteRepositories);
        } else {
            moduleArts = cp.getModuleArtifactsFromLilyRuntimeConfig(dependencyArtifacts, wiringXmlResource,
                    mavenProjectBuilder, remoteRepositories);
        }

        List allRemoteRepos = new ArrayList();
        allRemoteRepos.addAll(remoteRepositories);

        // In case the wiring.xml was read from an artifact, use that artifacts remote repositories to resolve
        // the modules listed in the wiring. TODO: This should in fact be applied further: the artifacts listed in the
        // modules classloader's should be retrieved from that module's artifact remoterepositories.

        // TODO we should filter out repos pointing to the same URI
        if (moduleArts.remoteRepositories != null) {
            allRemoteRepos.addAll(moduleArts.remoteRepositories);
        }

        cp.getAllArtifacts(moduleArts.artifacts, allRemoteRepos);
    }

    public class MyArtifactFilter implements LilyRuntimeProjectClasspath.ArtifactFilter {
        @Override
        public boolean include(Artifact artifact) {
            if (artifact.getGroupId().equals(projectGroupId)) {
                // Do not try to download artifacts from current project
                return false;
            } else if (sourceLocations.getSourceLocation(artifact.getGroupId(), artifact.getArtifactId()) != null) {
                // It is one of the artifacts of this project, hence the dependencies will have been
                // downloaded by Maven. Skip it.
                return false;
            }
            // This case is handled by the more generic case above
            //if (artifact.getGroupId().equals(projectGroupId) &&
            //        artifact.getArtifactId().equals(projectArtifactId) &&
            //        artifact.getVersion().equals(projectVersion)) {
            //    // Current project's artifact is not yet deployed, therefore do not treat it
            //    return false;
            //}
            return true;
        }
    }
}
