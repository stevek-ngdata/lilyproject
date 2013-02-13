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
package org.lilyproject.runtime.source;

import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.rapi.Mode;
import org.lilyproject.runtime.KauriRTException;
import org.apache.commons.jci.monitor.FilesystemAlterationListener;

import java.io.InputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URL;
import java.net.MalformedURLException;

public class JarModuleSource implements ModuleSource {
    private File file;
    private JarFile jarFile;
    private JarResource resourcesRoot = new JarResource(null);
    private JarResource classpathRoot = new JarResource(null);
    private JarEntry classLoaderConfig;

    private static final String RESOURCES_PATH = "LILY-INF/";
    private static final Pattern SPRING_COMMON_CONF_PATTERN = Pattern.compile("LILY-INF/spring/[^/]*\\.xml");
    private static final String KAURI_CLASSLOADER_CONF = "LILY-INF/classloader.xml";

    protected JarModuleSource(File file) throws IOException {
        this.file = file;
        init();
    }

    private void init() throws IOException {
        jarFile = new JarFile(file);

        // Find the relevant entries in the zip file
        Enumeration<JarEntry> jarEntries = jarFile.entries();
        while (jarEntries.hasMoreElements()) {
            JarEntry jarEntry = jarEntries.nextElement();
            String name = jarEntry.getName();

            if (name.equals(KAURI_CLASSLOADER_CONF)) {
                classLoaderConfig = jarEntry;
                continue;
            }

            if (name.startsWith(RESOURCES_PATH)) {
                String[] pathParts = parsePath(name.substring(RESOURCES_PATH.length()));
                createEntry(resourcesRoot, pathParts, jarEntry);
            } else {
                String[] pathParts = parsePath(name);
                createEntry(classpathRoot, pathParts, jarEntry);
            }
        }
    }

    public List<SpringConfigEntry> getSpringConfigs(Mode mode) {
        Pattern modeSpecificSpringPattern = Pattern.compile("LILY-INF/spring/" + mode.getName() + "/[^/]*\\.xml");
        List<SpringConfigEntry> result = new ArrayList<SpringConfigEntry>();
        final JarFile jarFile = this.jarFile;

        Enumeration<JarEntry> jarEntries = jarFile.entries();
        while (jarEntries.hasMoreElements()) {
            final JarEntry jarEntry = jarEntries.nextElement();
            final String name = jarEntry.getName();

            Matcher matcher1 = SPRING_COMMON_CONF_PATTERN.matcher(name);
            Matcher matcher2 = modeSpecificSpringPattern.matcher(name);
            if (matcher1.matches() || matcher2.matches()) {
                result.add(new SpringConfigEntry() {
                    public String getLocation() {
                        return name;
                    }

                    public InputStream getStream() throws IOException {
                        return jarFile.getInputStream(jarEntry);
                    }
                });
            }
        }

        return result;
    }

    private String[] parsePath(String path) {
        String[] parts = path.split("/");

        // Drop empty parts
        List<String> result = new ArrayList<String>(parts.length);
        for (String part : parts) {
            if (part.length() > 0)
                result.add(part);
        }

        return result.toArray(new String[result.size()]);
    }

    private JarResource createEntry(JarResource root, String[] pathParts, JarEntry jarEntry) {
        JarResource current = root;
        for (String part : pathParts) {
            JarResource child = current.getChild(part);
            if (child == null) {
                child = new JarResource(jarEntry);
                current.addChild(part, child);
            }
            current = child;
        }

        return current;
    }

    private JarResource findEntry(JarResource root, String[] pathParts) {
        JarResource current = root;
        for (String part : pathParts) {
            if (!current.isDirectory())
                return null;
            current = current.getChild(part);
            if (current == null)
                return null;
        }
        return current;
    }

    public Resource getResource(String path) {
        String[] pathParts = parsePath(path);
        JarResource resource = findEntry(resourcesRoot, pathParts);
        return resource;
    }

    public Resource getClasspathResource(String path) {
        String[] pathParts = parsePath(path);
        JarResource resource = findEntry(classpathRoot, pathParts);
        return resource;
    }

    public InputStream getClassLoaderConfig() throws IOException {
        return classLoaderConfig != null ? jarFile.getInputStream(classLoaderConfig) : null;
    }

    public File getClassPathEntry() {
        return file;
    }

    public boolean supportsListening() {
        return false;
    }

    public boolean isSourceMode() {
        return false;
    }

    public void addResourceListener(String path, FilesystemAlterationListener listener) {
    }

    public void removeResourceListener(FilesystemAlterationListener listener) {
    }

    public void dispose() throws IOException {
        if (jarFile != null) {
            jarFile.close();
        }
    }

    private class JarResource implements Resource {
        private Map<String, JarResource> children;
        private JarEntry jarEntry;

        private JarResource(JarEntry jarEntry) {
            this.jarEntry = jarEntry;
            if (jarEntry == null /* for the root */ || jarEntry.isDirectory())
                children = new HashMap<String, JarResource>();
        }

        public InputStream getInputStream() throws IOException {
            return jarFile.getInputStream(jarEntry);
        }

        public boolean isDirectory() {
            return children != null;
        }

        private JarResource getChild(String name) {
            if (children != null)
                return children.get(name);
            else
                throw new RuntimeException("Cannot get the children of a non-directory entry.");
        }

        private void addChild(String name, JarResource entry) {
            if (children.containsKey(name))
                throw new RuntimeException("There is already a child with the name \"" + name + "\".");
            children.put(name, entry);
        }

        public Collection<String> getChildren() {
            return Collections.unmodifiableCollection(children.keySet());
        }

        public URL getURL() {
            String path = "jar:" + file.toURI().toString() + "!/" + jarEntry.getName();
            try {
                return new URL(path);
            } catch (MalformedURLException e) {
                throw new KauriRTException("Error constructing URL for jar file entry " + path, e);
            }
        }

        public long lastModified() {
            return jarEntry.getTime();
        }
    }
}
