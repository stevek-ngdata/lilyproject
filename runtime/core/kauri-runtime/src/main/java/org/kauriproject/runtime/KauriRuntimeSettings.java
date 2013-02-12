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
package org.kauriproject.runtime;

import java.util.Set;

import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.model.*;
import org.kauriproject.runtime.configuration.ConfManager;

/**
 * Stuff needed to bootstrap a KauriRuntime instance.
 *
 */
public class KauriRuntimeSettings {
    private ArtifactRepository repository;
    private boolean enableArtifactSharing = true;
    private ConfManager confManager;
    private Set<String> disabledModuleIds;
    private KauriRuntimeModel model;
    private SourceLocations sourceLocations;
    private boolean disableServerConnectors;

    public KauriRuntimeSettings() {
    }

    public ArtifactRepository getRepository() {
        return repository;
    }

    public void setRepository(ArtifactRepository repository) {
        this.repository = repository;
    }

    public boolean getEnableArtifactSharing() {
        return enableArtifactSharing;
    }

    public void setEnableArtifactSharing(boolean enableArtifactSharing) {
        this.enableArtifactSharing = enableArtifactSharing;
    }

    public ConfManager getConfManager() {
        return confManager;
    }

    public void setConfManager(ConfManager confManager) {
        this.confManager = confManager;
    }

    public Set<String> getDisabledModuleIds() {
        return disabledModuleIds;
    }

    public void setDisabledModuleIds(Set<String> disabledModuleIds) {
        this.disabledModuleIds = disabledModuleIds;
    }

    public KauriRuntimeModel getModel() {
        return model;
    }

    /**
     * This might get removed: don't use it!
     *
     * <p>If you have a good use-case, let us know.
     */
    public void setModel(KauriRuntimeModel model) {
        this.model = model;
    }

    public SourceLocations getSourceLocations() {
        return sourceLocations;
    }

    public void setSourceLocations(SourceLocations sourceLocations) {
        this.sourceLocations = sourceLocations;
    }

    public boolean getDisableServerConnectors() {
        return disableServerConnectors;
    }

    /**
     * Disables server connectors regardless of what is specified in the connectors
     * configuration. Useful for embedded scenario's.
     */
    public void setDisableServerConnectors(boolean disableServerConnectors) {
        this.disableServerConnectors = disableServerConnectors;
    }
}
