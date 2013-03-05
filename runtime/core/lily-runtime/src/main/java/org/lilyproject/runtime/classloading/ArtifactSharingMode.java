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

/**
 * Defines how artifacts may be shared between the classloaders of multiple
 * modules.
 */
public enum ArtifactSharingMode {
    /**
     * Artifact that should not be in the shared classloader, because it is private
     * implementation. Typically used for the specific implementation classes of
     * the functionality provided by the module.
     */
    PROHIBITED("prohibited"),
    /**
     * Artifact that must be in the shared classloader, typically API of exported
     * services and any classes used by those.
     */
    REQUIRED("required"),
    /**
     * Artifact that can be put in the shared classloader, but doesn't need to.
     * The system can add the artifact to the shared classloader if multiple modules
     * use the artifact, and they all use the same version. However, things should just
     * as well work when they are in the module-specific classloader. Typically used
     * for all sorts of (third party) library code.
     */
    ALLOWED("allowed");

    private final String name;

    private ArtifactSharingMode(String name) {
        this.name = name;
    }


    public String toString() {
        return name;
    }

    public static ArtifactSharingMode fromString(String name) {
        if (PROHIBITED.name.equals(name))
            return PROHIBITED;
        else if (REQUIRED.name.equals(name))
            return REQUIRED;
        else if (ALLOWED.name.equals(name))
            return ALLOWED;
        else
            throw new RuntimeException("Invalid artifact sharing mode: " + name);
    }
}
