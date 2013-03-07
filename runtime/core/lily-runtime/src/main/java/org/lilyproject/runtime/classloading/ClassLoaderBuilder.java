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
package org.lilyproject.runtime.classloading;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.repository.ArtifactNotFoundException;
import org.lilyproject.runtime.repository.ArtifactRepository;

public class ClassLoaderBuilder {
    private static Map<String, ClassLoader> classLoaderCache;
    private static final boolean classLoaderCacheEnabled;
    static {
        classLoaderCacheEnabled = System.getProperty("lilyruntime.cacheclassloaders") != null;
        if (classLoaderCacheEnabled) {
            classLoaderCache = new HashMap<String, ClassLoader>();
        }
    }

    private ClassLoaderBuilder() {
    }

    public static synchronized ClassLoader build(List<ClasspathEntry> classpathEntries, ClassLoader parentClassLoader,
            ArtifactRepository repository) throws ArtifactNotFoundException, MalformedURLException {

        if (classLoaderCacheEnabled) {
            //
            // About the ClassLoader cache:
            //  The ClassLoader cache was introduced to handle leaks in the 'Perm Gen' and 'Code Cache'
            //  JVM memory spaces when repeatedly restarting Lily Runtime within the same JVM. In such cases,
            //  on each restart Lily Runtime would construct new class loaders, hence the classes loaded through
            //  them would be new and would stress those JVM memory spaces.
            //
            //  The solution here is good enough for what it is intended to do (restarting the same Lily Runtime
            //  app, unchanged, many times). There is no cache cleaning and the cache key calculation
            //  is not perfect, so if you would enable the cache when starting a wide variety of Lily Runtime
            //  apps within one VM or for reloading changed Lily Runtime apps, it could be problematic.
            //
            StringBuilder cacheKeyBuilder = new StringBuilder(2000);
            for (ClasspathEntry entry : classpathEntries) {
                cacheKeyBuilder.append(entry.getArtifactRef()).append("\u2603" /* unicode snowman as separator */);
            }

            // Add some identification of the parent class loader
            if (parentClassLoader instanceof URLClassLoader) {
                for (URL url : ((URLClassLoader)parentClassLoader).getURLs()) {
                    cacheKeyBuilder.append(url.toString());
                }
            }

            String cacheKey = cacheKeyBuilder.toString();

            ClassLoader classLoader = classLoaderCache.get(cacheKey);
            if (classLoader == null) {
                Log log = LogFactory.getLog(ClassLoaderBuilder.class);
                log.debug("Creating and caching a new classloader");
                classLoader = create(classpathEntries, parentClassLoader, repository);
                classLoaderCache.put(cacheKey, classLoader);
            } else if (classLoader.getParent() != parentClassLoader) {
                Log log = LogFactory.getLog(ClassLoaderBuilder.class);
                log.error("Lily Runtime ClassLoader cache: parentClassLoader of cache ClassLoader is different" +
                        " from the specified one. Returning the cached one anyway.");
            }

            return classLoader;
        } else {
            return create(classpathEntries, parentClassLoader, repository);
        }
    }

    private static ClassLoader create(List<ClasspathEntry> classpathEntries, ClassLoader parentClassLoader,
            ArtifactRepository repository) throws ArtifactNotFoundException, MalformedURLException {
        List<URL> classpath = new ArrayList<URL>();

        for (ClasspathEntry cpEntry : classpathEntries) {
            File resolvedFile = cpEntry.getArtifactRef().resolve(repository);
            classpath.add(resolvedFile.toURL());
        }

        return new URLClassLoader(classpath.toArray(new URL[classpath.size()]), parentClassLoader);
    }
}
