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
package org.lilyproject.runtime.runtime.rapi;

import org.apache.commons.jci.monitor.FilesystemAlterationListener;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.net.URL;                 

/**
 * Source of a module, i.e. where the system gets the content of the module
 * from (can be a jar, a directory, ...).
 */
public interface ModuleSource {
    /**
     * Returns null if the resource does not exist.
     *
     * @parm path slash-separated path. Is always relative, it makes no difference
     *            if the path starts with a slash or not. Use empty string to get
     *            the root resource.
     */
    Resource getResource(String path);

    Resource getClasspathResource(String path);

    /**
     * Returns null if there is no classloader config.
     * The caller is responsible for closing the input stream reliably (try-finally).
     */
    InputStream getClassLoaderConfig() throws IOException;

    List<SpringConfigEntry> getSpringConfigs(Mode mode);

    File getClassPathEntry();

    /**
     * Returns true if this ModuleSource supports listening for resource
     * changes by registering a listener to {@link #addResourceListener}.
     */
    boolean supportsListening();

    /**
     * Returns true if this module source runs directly on top of a source
     * tree. This can be used to adjust application behavior. Just to give
     * an example: sending debug-enabled versions of Javascript
     * to the browser. This should never be true for ModuleSource implementations
     * which are intended to be used in production.
     */
    boolean isSourceMode();

    /**
     * Adds a listener for the given resource path. Will do nothing if this module
     * doesn't support listening for changes.
     */
    void addResourceListener(String path, FilesystemAlterationListener listener);

    void removeResourceListener(FilesystemAlterationListener listener);

    /**
     * Allows the ModuleSource to clean up resources.
     */
    public void dispose() throws Exception;

    public interface Resource {
        InputStream getInputStream() throws IOException;

        Collection<String> getChildren();

        boolean isDirectory();

        URL getURL();

        long lastModified();
    }

    public interface SpringConfigEntry {
        /**
         * Location of the spring file, only intended to include in error/informational messages.
         */
        String getLocation();

        /**
         * Open an input stream. The caller is responsible for closing the stream (try-finally).
         */
        InputStream getStream() throws IOException;
    }
}
