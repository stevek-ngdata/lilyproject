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
package org.lilyproject.runtime.runtime.repository;

import java.io.File;

/**
 * A reference to a build artifact, typically a jar, possibly located in a {@link ArtifactRepository}.
 *
 * <p>Implementation should provide equals, hashcode and toString.
 */
public interface ArtifactRef {
    /**
     * Returns a string which uniquely identifies the non-versioned artifact (across
     * different ArtifactRef implementations).
     */
    String getId();

    String getVersion();

    /**
     * Makes a clone of this artifact for the specified version.
     */
    ArtifactRef clone(String version);

    File resolve(ArtifactRepository repository) throws ArtifactNotFoundException;

    /**
     * Should return a user-recognizable identification of this artifact.
     */
    String toString();
}
