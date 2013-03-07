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
import java.util.ArrayList;
import java.util.List;

public class ChainedMaven2StyleArtifactRepository extends BaseArtifactRepository {
    List<ArtifactRepository> repositories = new ArrayList<ArtifactRepository>();

    public ChainedMaven2StyleArtifactRepository(String locations) {
        String[] paths = locations.split(",");
        for (String path : paths) {
            path = path.trim();
            if (path.length() > 0) {
                repositories.add(new Maven2StyleArtifactRepository(new File(path)));
            }
        }
    }

    public ResolvedArtifact tryResolve(String groupId, String artifactId, String classifier, String version) throws ArtifactNotFoundException {
        List<String> searchLocations = new ArrayList<String>();

        File file = null;
        for (ArtifactRepository repository : repositories) {
            ResolvedArtifact artifact = repository.tryResolve(groupId, artifactId, classifier, version);
            searchLocations.addAll(artifact.getSearchedLocations());
            if (artifact.exists()) {
                file = artifact.getFile();
                break;
            }
        }

        return new ResolvedArtifact(file, searchLocations, file != null);
    }
}
