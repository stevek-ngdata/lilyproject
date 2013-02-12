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

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.kauriproject.runtime.configuration.ConfManagerImpl;
import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.repository.ChainedMaven2StyleArtifactRepository;

/**
 * Helper class for creating a KauriRuntime. This is intended to be the most
 * high-level interface for embedded creation of the repository.
 */
public class KauriRuntimeHelper {
    public static KauriRuntimeSettings createSettings(File confDir, String mavenRepoLocation) throws Exception {
        return createSettings(Collections.singletonList(confDir), mavenRepoLocation);
    }

    public static KauriRuntimeSettings createSettings(List<File> confDirs, String mavenRepoLocation) throws Exception {
        KauriRuntimeSettings settings = new KauriRuntimeSettings();
        settings.setConfManager(new ConfManagerImpl(confDirs));

        ArtifactRepository artifactRepository = new ChainedMaven2StyleArtifactRepository(mavenRepoLocation);
        settings.setRepository(artifactRepository);

        return settings;
    }

    public static KauriRuntime createRuntime(KauriRuntimeSettings settings) throws Exception {
        KauriRuntime runtime = new KauriRuntime(settings);
        runtime.start();
        return runtime;
    }
}
