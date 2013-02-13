/*
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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

import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.repository.ArtifactNotFoundException;
import org.lilyproject.runtime.repository.ArtifactRef;

import java.net.MalformedURLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class ClassLoadingConfigImpl implements ClassLoadingConfig {
    private List<ClasspathEntry> entries;
    private List<ClasspathEntry> sharedEntries = new ArrayList<ClasspathEntry>();
    private ArtifactRepository repository;

    public ClassLoadingConfigImpl(List<ClasspathEntry> classpathEntries, ArtifactRepository repository) {
        this.entries = classpathEntries;
        this.repository = repository;
    }

    public List<ClasspathEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public void enableSharing(ArtifactRef artifact) {
        for (ClasspathEntry entry : entries) {
            if (entry.getArtifactRef().getId().equals(artifact.getId())) { // comparing artifacts ignoring version
                sharedEntries.add(entry);
            }
        }
    }

    public ClassLoader getClassLoader(ClassLoader parentClassLoader) throws MalformedURLException, ArtifactNotFoundException {
        return ClassLoaderBuilder.build(getUsedClassPath(), parentClassLoader, repository);
    }

    public List<ClasspathEntry> getUsedClassPath() {
        List<ClasspathEntry> artifacts = new ArrayList<ClasspathEntry>();
        for (ClasspathEntry entry : entries) {
            if (!sharedEntries.contains(entry))
                artifacts.add(entry);
        }
        return artifacts;
    }

}
