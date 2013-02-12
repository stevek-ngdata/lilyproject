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

import org.apache.maven.project.MavenProjectBuilder;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.artifact.metadata.ArtifactMetadataSource;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.commons.lang.text.StrSubstitutor;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;
import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;
import java.util.regex.Matcher;
import java.io.*;
import java.nio.channels.FileChannel;

public abstract class AbstractPackageMojo extends AbstractMojo {
    /**
     * Location of the conf directory.
     *
     * @parameter expression="${basedir}/conf"
     * @required
     */
    protected String confDirectory;

    /**
     * Maven Project Builder component.
     *
     * @component
     */
    protected MavenProjectBuilder projectBuilder;

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
     * Artifact Resolver component.
     *
     * @component
     */
    protected ArtifactMetadataSource metadataSource;

    /**
     * The Kauri version to employ.
     *
     * @parameter
     */
    protected String kauriVersion;

    protected XPathFactory xpathFactory = XPathFactory.newInstance();

    protected FileFilter confDirFilter = new FileFilter() {
        public boolean accept(File pathname) {
            String name = pathname.getName();
            return !name.startsWith(".") && !name.equals("CVS");
        }
    };

    protected void init() throws MojoExecutionException {
        if (kauriVersion == null)
            determineKauriVersion();
    }

    protected void determineKauriVersion() throws MojoExecutionException {
        String pomPropsPath = "META-INF/maven/org.kauriproject/kauri-package-plugin/pom.properties";
        InputStream is = getClass().getClassLoader().getResourceAsStream(pomPropsPath);
        if (is == null) {
            throw new MojoExecutionException("Could not find the resource containing the Kauri version information at " + pomPropsPath);
        }

        Properties pomProps = new Properties();
        try {
            pomProps.load(is);
        } catch (IOException e) {
            throw new MojoExecutionException("Error reading pom properties from " + pomPropsPath, e);
        }
        this.kauriVersion = pomProps.getProperty("version");
    }

    protected void createRepository(Set<Artifact> artifacts, String repoDir) throws MojoExecutionException {
        getLog().info("Creating the embedded artifact repository containing " + artifacts.size() + " artifacts.");
        for (Artifact artifact : artifacts) {
            String groupPath = artifact.getGroupId().replaceAll("\\.", Matcher.quoteReplacement("/"));
            File targetDir = new File(repoDir + groupPath + "/"
                    + artifact.getArtifactId() + "/" + artifact.getVersion() + "/");
            targetDir.mkdirs();
            String fileName = artifact.getArtifactId() + "-" + artifact.getVersion();
            if(artifact.hasClassifier()) {
                fileName += "-" + artifact.getClassifier();
            }
            copyFile(artifact.getFile(), new File(targetDir, fileName + ".jar"));
        }
    }

    protected void copyFile(File fromFile, File toFile) throws MojoExecutionException {
        try {
            FileInputStream srcFis = new FileInputStream(fromFile);
            FileOutputStream destFos = new FileOutputStream(toFile);
            FileChannel srcChannel = srcFis.getChannel();
            FileChannel dstChannel = destFos.getChannel();
            dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
            srcChannel.close();
            dstChannel.close();
            srcFis.close();
            destFos.close();
        } catch (Throwable t) {
            throw new MojoExecutionException("Failed to copy file from " + fromFile + " to " + toFile, t);
        }
    }

    protected void copyResource(String path, File toFile) throws MojoExecutionException {
        copyResource(path, toFile, null);
    }

    /**
     *
     * @param properties if not null, will be used to perform property replacement. In this
     *                   case, character-interpretion of the stream will happen.
     */
    protected void copyResource(String path, File toFile, Map<String, String> properties) throws MojoExecutionException {
        String fullPath = "org/kauriproject/tools/plugin/packaging/" + path;
        InputStream is = getClass().getClassLoader().getResourceAsStream(fullPath);
        if (is == null)
            throw new MojoExecutionException("Classpath resource does not exist: " + fullPath);

        try {
            OutputStream os = new FileOutputStream(toFile);

            if (properties != null) {
                filter(is, os, properties);
            } else {
                byte[] buffer = new byte[8012];
                int read;
                while ((read = is.read(buffer)) != -1) {
                    os.write(buffer, 0, read);
                }
            }

            is.close();
            os.close();
        } catch (Throwable t) {
            throw new MojoExecutionException("Failed to copy classpath resource " + path + " to file " + toFile, t);
        }
    }

    protected void filter(InputStream is, OutputStream os, Map<String, String> properties) throws IOException {
        // Note: using default encoding for reader, making the assumption that the files
        // we process here won't contain special characters anyway
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(os));

        StrSubstitutor substitutor = new StrSubstitutor(properties);

        String line;
        while ((line = br.readLine()) != null) {
            writer.println(substitutor.replace(line));
        }

        writer.flush();
    }

    protected Set<Artifact> getModuleArtifactsFromKauriConfig() throws MojoExecutionException {
        File configFile = new File(confDirectory, "kauri/wiring.xml");
        Document configDoc;
        try {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                configDoc = parse(fis);
            } finally {
                if (fis != null)
                    fis.close();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Error reading kauri XML configuration from " + configFile, e);
        }

        return getArtifacts(configDoc, "/*/modules/artifact", "wiring.xml");
   }

    protected Set<Artifact> getClassPathArtifacts(Artifact moduleArtifact) throws MojoExecutionException {
        String entryPath = "KAURI-INF/classloader.xml";
        ZipFile zipFile = null;
        InputStream is = null;
        Document classLoaderDocument;
        try {
            zipFile = new ZipFile(moduleArtifact.getFile());
            ZipEntry zipEntry = zipFile.getEntry(entryPath);
            if (zipEntry == null) {
                getLog().debug("No " + entryPath + " found in " + moduleArtifact);
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

        return getArtifacts(classLoaderDocument, "/classloader/classpath/artifact", "classloader.xml from module " + moduleArtifact);
    }

    protected Set<Artifact> getArtifacts(Document configDoc, String artifactXPath, String sourceDescr) throws MojoExecutionException {
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
            if (version.equals("") && groupId.startsWith("org.kauriproject"))
                version = kauriVersion;
            if (classifier.equals(""))
                classifier = null;

            Artifact artifact = artifactFactory.createArtifactWithClassifier(groupId, artifactId, version, "jar", classifier);
            if (!artifacts.contains(artifact)) {
                try {
                    resolver.resolve(artifact, remoteRepositories, localRepository);
                } catch (Exception e) {
                    throw new MojoExecutionException("Error resolving artifact listed in " + sourceDescr + ": " + artifact, e);
                }
                artifacts.add(artifact);
            }
        }

        return artifacts;
    }

    protected Document parse(InputStream is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        return dbf.newDocumentBuilder().parse(is);
    }

    protected void deleteDirectory(File dir) throws IOException {
        if (!dir.exists())
            return;
        File files[] = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDirectory(file);
            } else {
                boolean deleted = file.delete();
                if (!deleted)
                    throw new IOException("Unable to delete file " + file.getAbsolutePath());
            }
        }

        boolean deleted = dir.delete();
        if (!deleted)
            throw new IOException("Unable to delete directory " + dir.getAbsolutePath());
    }

    protected void setExecutable(File file) throws MojoExecutionException {
        // rough check to see if it's a unix-type system
        if (System.getProperty("path.separator").equals(":")) {
            try {
                Process process = Runtime.getRuntime().exec(new String[] {"chmod", "u+x", file.getAbsolutePath()});
                process.waitFor();
                int exitValue = process.exitValue();
                if (exitValue != 0) {
                    getLog().warn("Could not make the following file executable, you might want to do it yourself: " + file.getAbsolutePath());
                }
            } catch (Exception e) {
                throw new MojoExecutionException("Error trying to make file executable: " + file, e);
            }
        }
    }
}
