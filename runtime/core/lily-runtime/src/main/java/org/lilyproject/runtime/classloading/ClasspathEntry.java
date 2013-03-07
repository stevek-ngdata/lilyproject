/*
 * Copyright 2013 NGDATA nv
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

import org.lilyproject.runtime.repository.ArtifactRef;
import org.lilyproject.runtime.rapi.ModuleSource;

public class ClasspathEntry {
    private final ArtifactRef artifactRef;
    private final ArtifactSharingMode sharingMode;
    private final ModuleSource moduleSource;

    /**
     *
     * @param moduleSource should/can only be specified if this classpath entry corresponds to the class files
     *                     contained within the module itself.
     */
    public ClasspathEntry(ArtifactRef artifactRef, ArtifactSharingMode sharingMode, ModuleSource moduleSource) {
        this.artifactRef = artifactRef;
        this.sharingMode = sharingMode;
        this.moduleSource = moduleSource;
    }

    public ArtifactRef getArtifactRef() {
        return artifactRef;
    }

    public ArtifactSharingMode getSharingMode() {
        return sharingMode;
    }

    public ModuleSource getModuleSource() {
        return moduleSource;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof ClasspathEntry))
            return false;

        ClasspathEntry other = (ClasspathEntry)obj;

        return artifactRef.equals(other.artifactRef) && sharingMode == other.sharingMode;
    }
}
