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
package org.lilyproject.runtime.repository;

import java.io.File;
import java.util.List;

public class ResolvedArtifact {
    private File file;
    private List<String> searchedLocations;
    private boolean exists;

    public ResolvedArtifact(File file, List<String> searchedLocations, boolean exists) {
        this.file = file;
        this.searchedLocations = searchedLocations;
        this.exists = exists;
    }

    public File getFile() {
        return file;
    }

    public List<String> getSearchedLocations() {
        return searchedLocations;
    }

    public boolean exists() {
        return exists;
    }
}
