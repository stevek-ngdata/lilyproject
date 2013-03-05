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
package org.lilyproject.runtime.source;

import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.rapi.Mode;
import org.apache.commons.jci.monitor.FilesystemAlterationMonitor;
import org.apache.commons.jci.monitor.FilesystemAlterationListener;

import java.io.*;
import java.util.Collection;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.net.URL;
import java.net.MalformedURLException;

public abstract class AbstractDirectoryModuleSource implements ModuleSource {
    private File dir;
    private File resourceDir;
    private File overrideResourceDir;
    private File springDir;
    private File classLoaderConfig;
    private File classPathEntry;
    private FilesystemAlterationMonitor fam;

    /**
     *
     * @param overrideResourceDir a directory that overrides the files from the normal resourceDir,
     *                            if they are newer. Optional, can be null.
     */
    public AbstractDirectoryModuleSource(File dir, File resourceDir, File overrideResourceDir, File springDir,
            File classLoaderConfig, File classPathEntry, FilesystemAlterationMonitor fam) throws IOException {
        this.dir = dir;
        this.resourceDir = resourceDir;
        this.overrideResourceDir = overrideResourceDir;
        this.springDir = springDir;
        this.classLoaderConfig = classLoaderConfig;
        this.classPathEntry = classPathEntry;
        this.fam = fam;
    }

    private static final Pattern LEADING_SLASHES_PATTERN = Pattern.compile("^(/)*");

    private static final FilenameFilter SPRING_FILE_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return name.endsWith(".xml");
        }
    };

    public List<SpringConfigEntry> getSpringConfigs(Mode mode) {
        List<File> springFiles = new ArrayList<File>();

        File[] commonFiles = springDir.listFiles(SPRING_FILE_FILTER);
        if (commonFiles != null)
            springFiles.addAll(Arrays.asList(commonFiles));

        File[] modeSpecificFiles = new File(springDir, mode.getName()).listFiles(SPRING_FILE_FILTER);
        if (modeSpecificFiles != null)
            springFiles.addAll(Arrays.asList(modeSpecificFiles));

        List<SpringConfigEntry> result = new ArrayList<SpringConfigEntry>();

        for (final File file : springFiles) {
            result.add(new SpringConfigEntry() {
                public String getLocation() {
                    return file.getAbsolutePath();
                }

                public InputStream getStream() throws IOException {
                    return new FileInputStream(file);
                }
            });
        }

        return result;
    }

    public ModuleSource.Resource getResource(String path) {
        // Paths should always be relative (this current code is not 'safe', only convenient in case of accidental start slashes)
        path = LEADING_SLASHES_PATTERN.matcher(path).replaceFirst("");
        File file = new File(resourceDir, path);

        if (!file.exists())
            return null;

        FileResource fileResource = new FileResource(file);

        if (overrideResourceDir != null) {
            File overrideFile = new File(overrideResourceDir, path);
            if (overrideFile.exists() && !overrideFile.isDirectory()
                    && overrideFile.lastModified() > file.lastModified()) {
                fileResource = new FileResource(overrideFile);
            }
        }

        return fileResource;
    }

    public Resource getClasspathResource(String path) {
        File file = new File(classPathEntry, path);
        if (!file.exists())
            return null;
        else
            return new FileResource(file);
    }

    public InputStream getClassLoaderConfig() throws IOException {
        if (!classLoaderConfig.exists())
            return null;
        return new FileInputStream(classLoaderConfig);
    }

    public File getClassPathEntry() {
        return classPathEntry;
    }

    public boolean supportsListening() {
        return true;
    }

    public void addResourceListener(String path, FilesystemAlterationListener listener) {
        File file = new File(resourceDir, path);
        fam.addListener(file, listener);
    }

    public void removeResourceListener(FilesystemAlterationListener listener) {
        fam.removeListener(listener);
    }

    public void dispose() throws Exception {
        // Nothing to dispose
    }

    private class FileResource implements ModuleSource.Resource {
        private File file;

        private FileResource(File file) {
            this.file = file;
        }

        public InputStream getInputStream() throws IOException {
            return new FileInputStream(file);
        }

        public Collection<String> getChildren() {
            return Arrays.asList(file.list());
        }

        public boolean isDirectory() {
            return file.isDirectory();
        }

        public URL getURL() {
            try {
                return file.toURL();
            } catch (MalformedURLException e) {
                throw new LilyRTException("Error constructing file URL for file " + file.getAbsolutePath(), e);
            }
        }

        public long lastModified() {
            return file.lastModified();
        }
    }
}
