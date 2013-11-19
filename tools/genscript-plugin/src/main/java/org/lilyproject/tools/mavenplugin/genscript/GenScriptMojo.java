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
package org.lilyproject.tools.mavenplugin.genscript;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.factory.ArtifactFactory;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.settings.Settings;

/**
 * @requiresDependencyResolution runtime
 * @goal genscript
 */
public class GenScriptMojo extends AbstractMojo {

    /**
     * A short name of the application without special characters. Casing does not matter, it will be
     * uppercased or lowercased depending on the context.
     *
     * @parameter
     * @required
     */
    private String applicationName;

    /**
     * @parameter
     */
    private List<Script> scripts;

    /**
     * @parameter
     */
    private List<Dependency> alternativeClasspath;

    /**
     * @parameter
     */
    private boolean includeProjectInClasspath = true;

    /**
     * @parameter
     */
    private List<Parameter> defaultCliArgs;

    /**
     * @parameter
     */
    private List<Parameter> defaultJvmArgs;

    /**
     * @parameter
     */
    private List<Parameter> classPathPrefix;

    /**
     * @parameter
     */
    private List<Parameter> beforeJavaHook;

    /**
     * @parameter default-value="${project.build.directory}"
     */
    private File devOutputDirectory;

    /**
     * @parameter default-value="${project.build.directory}/dist-scripts"
     */
    private File distOutputDirectory;

    private Map<Mode, File> outputDirectories = new EnumMap<Mode, File>(Mode.class);

    /**
     * @parameter default-value="${settings}"
     * @readonly
     * @required
     */
    private Settings settings;

    /**
     * @parameter default-value="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    /**
     * Maven Artifact Factory component.
     *
     * @component
     */
    protected ArtifactFactory artifactFactory;

    /**
     * Artifact Resolver component.
     *
     * @component
     */
    protected ArtifactResolver resolver;

    /**
     * Remote repositories used for the project.
     *
     * @parameter default-value="${project.remoteArtifactRepositories}"
     * @required
     * @readonly
     */
    protected List remoteRepositories;

    /**
     * Local Repository.
     *
     * @parameter default-value="${localRepository}"
     * @required
     * @readonly
     */
    protected ArtifactRepository localRepository;

    private ArtifactRepositoryLayout m2layout = new DefaultRepositoryLayout();

    enum Platform {
        UNIX("/", ":", "$", "", ""), WINDOWS("\\", ";", "%", "%", ".bat");

        Platform(String fileSeparator, String pathSeparator, String envPrefix, String envSuffix, String extension) {
            this.fileSeparator = fileSeparator;
            this.pathSeparator = pathSeparator;
            this.envPrefix = envPrefix;
            this.envSuffix = envSuffix;
            this.extension = extension;
        }

        private String fileSeparator;
        private String pathSeparator;
        private String envPrefix;
        private String envSuffix;
        private String extension;
    }

    enum Mode {
        DEV("-dev"),
        DIST("");

        Mode(String templateSuffix) {
            this.templateSuffix = templateSuffix;
        }

        private String templateSuffix;
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        outputDirectories.put(Mode.DEV, devOutputDirectory);
        outputDirectories.put(Mode.DIST, distOutputDirectory);

        try {
            for (Script script: scripts) {
                generateScripts(script);
            }
        } catch (IOException ioe) {
            throw new MojoFailureException("Failed to generate script ", ioe);
        }
    }

    private void generateScripts(Script script) throws IOException, MojoExecutionException {
        devOutputDirectory.mkdirs();
        distOutputDirectory.mkdirs();

        for (Mode mode : Mode.values()) {
            for (Platform platform : Platform.values()) {
                String cp = generateClassPath(mode == Mode.DEV, platform);

                File scriptDir = outputDirectories.get(mode);
                scriptDir.mkdirs();

                File scriptFile = new File(scriptDir, script.getBasename().concat(platform.extension));

                generateScript(scriptFile, platform.name().toLowerCase() + mode.templateSuffix + ".template",
                        script.getMainClass(), cp, platform, mode);

                if (new File("/bin/chmod").exists()) {
                    Runtime.getRuntime().exec("/bin/chmod a+x " + scriptFile.getAbsolutePath());
                }
            }
        }
    }

    private void generateScript(File outputFile, String template, String mainClass,
            String classPath, Platform platform, Mode mode) throws IOException {

        InputStream is = getClass().getResourceAsStream("/org/lilyproject/tools/mavenplugin/genscript/".concat(template));
        String result = streamToString(is);


        String defaultCliArgs = getParameter(this.defaultCliArgs, platform, mode, "");

        String defaultJvmArgs = getParameter(this.defaultJvmArgs, platform, mode, "");

        String beforeJavaHook = getParameter(this.beforeJavaHook, platform, mode, "");

        String classPathPrefix = getParameter(this.classPathPrefix, platform, mode, "");

        String separator = "**";
        result = result.replaceAll(Pattern.quote(separator.concat("CLASSPATH").concat(separator)), Matcher.quoteReplacement(classPath)).
            replaceAll(Pattern.quote(separator.concat("CLASSPATH_PREFIX").concat(separator)), Matcher.quoteReplacement(classPathPrefix)).
            replaceAll(Pattern.quote(separator.concat("MAINCLASS").concat(separator)), Matcher.quoteReplacement(mainClass)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_CLI_ARGS").concat(separator)), Matcher.quoteReplacement(defaultCliArgs)).
            replaceAll(Pattern.quote(separator.concat("DEFAULT_JVM_ARGS").concat(separator)), Matcher.quoteReplacement(defaultJvmArgs)).
            replaceAll(Pattern.quote(separator.concat("BEFORE_JAVA_HOOK").concat(separator)), Matcher.quoteReplacement(beforeJavaHook)).
            replaceAll(Pattern.quote(separator.concat("APPNAME").concat(separator)), Matcher.quoteReplacement(applicationName.toUpperCase())).
            replaceAll(Pattern.quote(separator.concat("APPNAME_LC").concat(separator)), Matcher.quoteReplacement(applicationName.toLowerCase()));

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        writer.write(result);
        writer.close();
    }

    private String streamToString(InputStream in) throws IOException {
        StringBuilder out = new StringBuilder();
        byte[] b = new byte[4096];
        for (int n; (n = in.read(b)) != -1;) {
            out.append(new String(b, 0, n));
        }
        return out.toString();
    }

    private String generateClassPath(boolean isDevelopment, Platform platform) throws MojoExecutionException {
        // The classpath is generated such that there is no trailing path separator: otherwise this can cause
        // the current working directory to be added to the classpath unintentionally
        StringBuilder result = new StringBuilder();
        ArtifactRepositoryLayout layout = m2layout;
        String basePath = isDevelopment ? settings.getLocalRepository() : platform.envPrefix.concat(applicationName.toUpperCase()).concat("_HOME").concat(platform.envSuffix).concat(platform.fileSeparator).concat("lib");

        for (Artifact artifact: getClassPath()) {
            if (result.length() > 0) {
                result.append(platform.pathSeparator);
            }
            result.append(basePath).append(platform.fileSeparator).append(artifactPath(artifact, platform));
        }

        if (includeProjectInClasspath) {
            if (result.length() > 0) {
                result.append(platform.pathSeparator);
            }

            // Disabled the isDevelopment treatment: otherwise in development, META-INF does not exist which
            // is used by Lily's CLI tools to read their version. Not sure if there is a good reason for this
            // behavior: it was probably inherited from Lily Runtime where it made sense to allow resources changes
            // to be picked up immediately.
            //if (isDevelopment) {
            //    result.append(project.getBuild().getOutputDirectory());
            //} else {
            result.append(basePath).append(platform.fileSeparator).append(artifactPath(project.getArtifact(), platform));
            //}
        }

        return result.toString();
    }

    private String artifactPath(Artifact artifact, Platform platform) {
        // pathOf always creates a path with slashes, irrespective of the current platform
        String artifactPath = pathOf(artifact);
        artifactPath = artifactPath.replaceAll("/", Matcher.quoteReplacement(platform.fileSeparator));
        return artifactPath;
    }

    private List<Artifact> getClassPath() throws MojoExecutionException {
        if (alternativeClasspath != null && alternativeClasspath.size() > 0) {
            return getAlternateClassPath();
        } else {
            return (List<Artifact>)project.getRuntimeArtifacts();
        }
    }

    private List<Artifact> getAlternateClassPath() throws MojoExecutionException {
        List<Artifact> result = new ArrayList<Artifact>();
        for (Dependency dependency : alternativeClasspath) {
            Artifact artifact = artifactFactory.createArtifactWithClassifier(dependency.getGroupId(),
                    dependency.getArtifactId(), dependency.getVersion(), "jar", dependency.getClassifier());
            try {
                resolver.resolve(artifact, remoteRepositories, localRepository);
            } catch (Exception e) {
                throw new MojoExecutionException("Error resolving artifact: " + artifact, e);
            }
            result.add(artifact);
        }
        return result;
    }

    private String getParameter(List<Parameter> parameters, Platform platform, Mode mode, String defaultValue) {
        if (parameters == null) {
            return defaultValue;
        }

        for (Parameter parameter : parameters) {
            if (parameter.platform.toUpperCase().equals(platform.name()) && parameter.mode.toUpperCase().equals(mode.name())) {
                return parameter.value;
            }
        }
        return defaultValue;
    }

    private static final char PATH_SEPARATOR = '/';
    private static final char GROUP_SEPARATOR = '.';
    private static final char ARTIFACT_SEPARATOR = '-';

    /**
     * disclaimer: this method has been copied from the DefaultRepositoryLayout of the Maven
     * source tree and is Apache licensed. It was changed to use getBaseVersion() instead
     * of getVersion() on the artifact.
     */
    private static String pathOf(Artifact artifact) {
        ArtifactHandler artifactHandler = artifact.getArtifactHandler();

        StringBuilder path = new StringBuilder();

        path.append(formatAsDirectory(artifact.getGroupId())).append(PATH_SEPARATOR);
        path.append(artifact.getArtifactId()).append(PATH_SEPARATOR);
        path.append(artifact.getBaseVersion()).append(PATH_SEPARATOR);
        // Lily change: Here we call getBaseVersion() instead of getVersion() because otherwise for snapshot artifacts
        // a timestamp suffix is included in the version (and our shell scripts & runtime classloader.xml's
        // refer to the plain snapshot version)
        path.append(artifact.getArtifactId()).append(ARTIFACT_SEPARATOR).append(artifact.getBaseVersion());

        if (artifact.hasClassifier()) {
            path.append(ARTIFACT_SEPARATOR).append(artifact.getClassifier());
        }

        if (artifactHandler.getExtension() != null && artifactHandler.getExtension().length() > 0) {
            path.append(GROUP_SEPARATOR).append(artifactHandler.getExtension());
        }

        return path.toString();
    }

    private static String formatAsDirectory( String directory ) {
        return directory.replace(GROUP_SEPARATOR, PATH_SEPARATOR);
    }
}
