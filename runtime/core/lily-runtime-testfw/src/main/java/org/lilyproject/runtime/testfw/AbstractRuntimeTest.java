/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.testfw;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.LilyRuntimeSettings;
import org.lilyproject.runtime.configuration.ConfManager;
import org.lilyproject.runtime.configuration.ConfManagerImpl;
import org.lilyproject.runtime.model.LilyRuntimeModel;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.repository.ArtifactNotFoundException;
import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.repository.Maven2StyleArtifactRepository;
import org.lilyproject.runtime.repository.RepoArtifactRef;
import org.lilyproject.runtime.repository.ResolvedArtifact;
import org.slf4j.bridge.SLF4JBridgeHandler;

public abstract class AbstractRuntimeTest extends TestCase {
    protected ArtifactRepository dummyRepository = new ArtifactRepository() {
        public File resolve(RepoArtifactRef artifactRef) throws ArtifactNotFoundException {
            return resolve(artifactRef.getGroupId(), artifactRef.getArtifactId(), artifactRef.getClassifier(), artifactRef.getVersion());
        }

        public File resolve(String groupId, String artifactId, String classifier, String version) throws ArtifactNotFoundException {
            throw new ArtifactNotFoundException(groupId, artifactId, classifier, version, Collections.<String>emptyList());
        }

        public ResolvedArtifact tryResolve(String groupId, String artifactId, String classifier, String version) throws ArtifactNotFoundException {
            return new ResolvedArtifact(new File("/dummy"), Collections.<String>emptyList(), false);
        }
    };

    protected static int HTTP_TEST_PORT = 40821;
    protected ArtifactRepository localRepository;
    
    // This localRepository property is set by Maven (or its test plugin)
    {
        String localRepositoryPath = System.getProperty("localRepository");
        if (localRepositoryPath == null)
            localRepositoryPath = System.getProperty("user.home") + "/.m2/repository";
        
        localRepository = new Maven2StyleArtifactRepository(new File(localRepositoryPath)) {
                public ResolvedArtifact tryResolve(String groupId, String artifactId, String classifier, String version) throws ArtifactNotFoundException {
                    // If the artifact is the one of the project in which this test is running
                    // then the artifact won't be in the repository yet, so first try the target
                    // dir (we don't know the name of the current project, so try this for all
                    // artifacts)
                    String basedir = System.getProperty("basedir");
                    File file = new File(basedir + "/target/" + artifactId + "-" + version + ".jar");
                    if (file.exists())
                        return new ResolvedArtifact(file, Collections.singletonList(file.getAbsolutePath()), true);
                    //else
                    return super.tryResolve(groupId, artifactId, classifier, version);
                }
            };
    }
    
    protected String projectVersion = System.getProperty("project.version"); // This property should be explictly set via test plugin configuration

    protected LilyRuntime runtime;

    private Set<File> createdTempDirs = new HashSet<File>();

    /**
     * Creates a module layout in a temporary directory based on the contents of a package.
     * This is a lightweight mechanism to create modules in testcases, to avoid the need
     * to e.g. set up a maven project for each test module.
     */
    public File createModule(String packageName) throws IOException {
        File moduleDir = createTempDir();

        String basedir = System.getProperty("basedir"); // This property is set by the Maven surefire:test goal
        if (basedir == null) { // we are not running in maven, assume the current working dir is the basedir:
            basedir = System.getProperty("user.dir");
        }

        String packageDir = packageName.replaceAll("\\.", "/");
        File resourceDir = new File(basedir + "/src/test/modulesrc/" + packageDir);
        copyChildren(resourceDir, moduleDir);

        File classesDir = new File(basedir + "/target/test-classes/" + packageDir);
        File classesDestDir = new File(moduleDir, packageDir);
        copyChildren(classesDir, classesDestDir);

        String commonClassesPath = "org/lilyproject/runtime/testmodules/common";
        File commonClassesDir = new File(basedir + "/target/test-classes/" + commonClassesPath);
        File commonClassesDestDir = new File(moduleDir, commonClassesPath);
        copyChildren(commonClassesDir, commonClassesDestDir);

        System.out.println("Temporary module created at " + moduleDir.getAbsolutePath());
        
        return moduleDir;
    }

    public File createTempDir() {
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
        createdTempDirs.add(dir);

        return dir;
    }

    private void copyChildren(File srcDir, File destDir) throws IOException {
        if (!srcDir.exists())
            return;
        if (!destDir.exists())
            destDir.mkdirs();

        File[] files = srcDir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                File childDestDir = new File(destDir, file.getName());
                childDestDir.mkdir();
                copyChildren(file, childDestDir);
            } else {
                copyFile(file, new File(destDir, file.getName()));
            }
        }
    }

    private void copyFile(File src, File dest) throws IOException {
        FileInputStream fis = new FileInputStream(src);
        FileOutputStream fos = new FileOutputStream(dest);
        byte[] buffer = new byte[8192];
        int read;

        while ((read = fis.read(buffer)) != -1) {
            fos.write(buffer, 0, read);
        }

        fis.close();
        fos.close();
    }

    /**
     * Creates a configuration dir based on a package name.
     */
    public File createConfDir(String confName) {
        String basedir = System.getProperty("basedir"); // This property is set by the Maven surefire:test goal
        if (basedir == null) { // we are not running in maven, assume the current working dir is the basedir:
            basedir = System.getProperty("user.dir");
        }

        File resourceDir = new File(basedir + "/src/test/confs/" + confName);
        return resourceDir;
    }

    protected LilyRuntimeModel getRuntimeModel() throws Exception {
        return null;
    }

    protected Mode getMode() {
        return Mode.getDefault();
    }

    protected boolean startRuntime() {
        return true;
    }

    protected ConfManager getConfManager() throws Exception {
        return new ConfManagerImpl(Collections.<File>emptyList());
    }

    protected void setUpLogging() {
        // Forward JDK logging to SLF4J
        java.util.logging.LogManager.getLogManager().reset();
        java.util.logging.LogManager.getLogManager().getLogger("").addHandler(new SLF4JBridgeHandler());
        java.util.logging.LogManager.getLogManager().getLogger("").setLevel(java.util.logging.Level.ALL);
        
        // By passing these system properties, you can easily enable a certain level
        // of debugging for a certain log category
        String consoleLoggingLevel = System.getProperty("console-logging");
        String consoleLogCategory = System.getProperty("console-log-category");

        String appenderName = "abstract-runtime-test-console-appender";
        Logger rootLogger = Logger.getRootLogger();
        if (rootLogger.getAppender(appenderName) == null) {
            ConsoleAppender consoleAppender = new ConsoleAppender();
            consoleAppender.setName(appenderName);
            consoleAppender.setLayout(new PatternLayout("[%t] %-5p %c - %m%n"));
            consoleAppender.activateOptions();
            rootLogger.addAppender(consoleAppender);

            if (consoleLoggingLevel != null) {
                Level level = null;
                if (consoleLoggingLevel.equalsIgnoreCase("trace"))
                    level = Level.TRACE;
                else if (consoleLoggingLevel.equalsIgnoreCase("debug"))
                    level = Level.DEBUG;
                else if (consoleLoggingLevel.equalsIgnoreCase("info"))
                    level = Level.INFO;
                else if (consoleLoggingLevel.equalsIgnoreCase("warn"))
                    level = Level.WARN;
                else if (consoleLoggingLevel.equalsIgnoreCase("error"))
                    level = Level.ERROR;
                else if (consoleLoggingLevel.equalsIgnoreCase("fatal"))
                    level = Level.FATAL;
                else
                    System.err.println("Unrecognized log level: " + consoleLoggingLevel);

                if (level != null) {
                    System.out.println("Setting console output for log level " + level.toString() + " on category " + consoleLogCategory);
                    Logger logger = consoleLogCategory == null ? Logger.getRootLogger() : Logger.getLogger(consoleLogCategory);
                    logger.setLevel(level);

                    if (consoleLogCategory != null)
                        rootLogger.setLevel(Level.WARN);
                }
            } else {
                rootLogger.setLevel(Level.WARN);
            }

        }
    }

    protected void setUp() throws Exception {
        setUpLogging();

        LilyRuntimeModel model = getRuntimeModel();
        if (model != null) {
            LilyRuntimeSettings settings = new LilyRuntimeSettings();
            settings.setModel(model);
            settings.setRepository(localRepository);
            settings.setConfManager(getConfManager());

            runtime = new LilyRuntime(settings);
            runtime.setMode(getMode());
            
            if (startRuntime())
                runtime.start();
        }
    }

    protected void tearDown() throws Exception {
        if (runtime != null)
            runtime.stop();
        deleteTempModuleDirs();
    }

    private void deleteTempModuleDirs() {
        for (File dir : createdTempDirs) {
            deleteDir(dir);
        }
    }

    protected void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : file.listFiles()) {
                    deleteDir(child);
                }
            }
        }
        file.delete();
    }
    
}
