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

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectBuilder;
import org.lilyproject.util.Version;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class KauriProjectClasspath {
    protected XPathFactory xpathFactory = XPathFactory.newInstance();
    private String lilyVersion;
    private ArtifactFilter filter;
    private ArtifactFactory artifactFactory;
    protected ArtifactResolver resolver;
    protected ArtifactRepository localRepository;
    private Log log;

    public KauriProjectClasspath(Log log, ArtifactFilter filter, ArtifactFactory artifactFactory,
            ArtifactResolver resolver, ArtifactRepository localRepository) {
        this.filter = filter;
        this.artifactFactory = artifactFactory;
        this.resolver = resolver;
        this.localRepository = localRepository;
        this.log = log;
        this.lilyVersion = Version.readVersion("org.lilyproject", "lily-runtime-plugin");
    }

    public Set<Artifact> getAllArtifacts(Set<Artifact> moduleArtifacts, List remoteRepositories)
            throws MojoExecutionException {
        Set<Artifact> result = new HashSet<Artifact>();
        result.addAll(moduleArtifacts);

        for (Artifact moduleArtifact : moduleArtifacts) {
            result.addAll(getClassPathArtifacts(moduleArtifact, remoteRepositories));
        }

        return result;
    }

    public ModuleArtifacts getModuleArtifactsFromKauriConfig(File confDirectory, List remoteRepos)
            throws MojoExecutionException {
        File configFile = new File(confDirectory, "runtime/wiring.xml");
        try {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);

                ModuleArtifacts result = new ModuleArtifacts();
                result.artifacts = getModuleArtifactsFromKauriConfig(fis, configFile.getAbsolutePath(),
                        remoteRepos);

                return result;
            } finally {
                if (fis != null)
                    fis.close();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading lily runtime XML configuration from " + configFile, e);
        }
    }

    public ModuleArtifacts getModuleArtifactsFromKauriConfig(Set<Artifact> dependencies, String wiringPath,
            MavenProjectBuilder mavenProjectBuilder, List remoteRepos)
            throws MojoExecutionException {

        try {
            // Search in the jars of all the direct dependencies of the project for the wiring file
            // (not sure if this won't be too slow? it's just to avoid the user having to specify the artifact)
            log.info("Searching " + dependencies.size() + " dependencies for " + wiringPath);
            for (Artifact artifact : dependencies) {
                if ("jar".equals(artifact.getType())) {
                    resolver.resolve(artifact, remoteRepos, localRepository);

                    ZipFile zipFile;
                    zipFile = new ZipFile(artifact.getFile());
                    try {
                        ZipEntry zipEntry = zipFile.getEntry(wiringPath);
                        if (zipEntry != null) {
                            log.info("Reading " + wiringPath + " from " + artifact.getFile());
                            Set<Artifact> moduleArtifacts = getModuleArtifactsFromKauriConfig(
                                    zipFile.getInputStream(zipEntry), wiringPath, remoteRepos);

                            MavenProject wiringSourceProject = mavenProjectBuilder.buildFromRepository(artifact,
                                    remoteRepos, localRepository);
                            List repositories = wiringSourceProject.getRemoteArtifactRepositories();

                            ModuleArtifacts result = new ModuleArtifacts();
                            result.artifacts = moduleArtifacts;
                            result.remoteRepositories = repositories;

                            return result;
                        }
                    } finally {
                        zipFile.close();
                    }
                }
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error searching/reading wiring.xml file from dependency jars.", e);
        }

        throw new MojoExecutionException("The wiring.xml was not found in the dependency jars at path " + wiringPath);
    }

    public Set<Artifact> getModuleArtifactsFromKauriConfig(InputStream wiringStream, String path, List remoteRepos)
            throws MojoExecutionException {
        Document configDoc;
        try {
            configDoc = parse(wiringStream);
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading lily runtime wiring from " + path, e);
        }

        return getArtifacts(configDoc, "/*/modules/artifact", path, remoteRepos);
    }

    public Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact, List remoteRepositories)
            throws MojoExecutionException {
        return getClassPathArtifacts(moduleArtifact, "LILY-INF/classloader.xml", remoteRepositories);
    }

    public Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact, String entryPath, List remoteRepos)
            throws MojoExecutionException {
        ZipFile zipFile = null;
        InputStream is = null;
        Document classLoaderDocument;
        try {
            zipFile = new ZipFile(moduleArtifact.getFile());
            ZipEntry zipEntry = zipFile.getEntry(entryPath);
            if (zipEntry == null) {
                log.debug("No " + entryPath + " found in " + moduleArtifact);
                return Collections.emptySet();
            } else {
                is = zipFile.getInputStream(zipEntry);
                classLoaderDocument = parse(is);
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading " + entryPath + " from " + moduleArtifact, e);
        } finally {
            if (is != null)
                try { is.close(); } catch (Exception e) { /* ignore */ }
            if (zipFile != null)
                try { zipFile.close(); } catch (Exception e) { /* ignore */ }
        }

        return getArtifacts(classLoaderDocument,
                "/classloader/classpath/artifact", "classloader.xml from module " + moduleArtifact, remoteRepos);
    }

    protected Set<Artifact> getArtifacts(Document configDoc, String artifactXPath, String sourceDescr,
            List remoteRepos) throws MojoExecutionException {
        Set<Artifact> artifacts = new HashSet<Artifact>();
        NodeList nodeList;
        try {
            nodeList = (NodeList)xpathFactory.newXPath().evaluate(artifactXPath, configDoc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new MojoExecutionException("Error resolving XPath expression " + artifactXPath + " on " + sourceDescr);
        }
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element el = (Element)nodeList.item(i);
            String groupId = el.getAttribute("groupId");
            String artifactId = el.getAttribute("artifactId");
            String version = el.getAttribute("version");
            String classifier = el.getAttribute("classifier");
            if (version.equals("") && groupId.startsWith("org.lilyproject"))
                version = lilyVersion;
            if (classifier.equals(""))
                classifier = null;

            Artifact artifact = artifactFactory.createArtifactWithClassifier(groupId, artifactId, version, "jar", classifier);

            if (filter == null || filter.include(artifact)) {
                if (!artifacts.contains(artifact)) {
                    if (resolver != null) {
                        try {
                            resolver.resolve(artifact, remoteRepos, localRepository);
                        } catch (Exception e) {
                            throw new MojoExecutionException("Error resolving artifact listed in " + sourceDescr + ": " + artifact, e);
                        }
                    }
                    artifacts.add(artifact);
                }
            }
        }

        return artifacts;
    }

    protected Document parse(InputStream is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        return dbf.newDocumentBuilder().parse(is);
    }

    public static interface ArtifactFilter {
        boolean include(Artifact artifact);
    }
}
