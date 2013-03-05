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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.module.build.VersionManager;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.repository.ArtifactRef;
import org.lilyproject.runtime.repository.ArtifactRepository;
import org.lilyproject.runtime.repository.FileArtifactRef;
import org.lilyproject.runtime.repository.RepoArtifactRef;
import org.lilyproject.util.xml.DocumentHelper;
import org.w3c.dom.Element;

/**
 * Builds a ClassLoadingConfig from XML.
 */
public class XmlClassLoaderBuilder {
    private final Log log = LogFactory.getLog(getClass());
    private Element element;
    private ModuleSource moduleSource;
    private ArtifactRepository repository;
    private VersionManager versionManager;

    public static ClassLoadingConfig build(Element element, ModuleSource moduleSource, ArtifactRepository repository, VersionManager versionManager) throws Exception {
        return new XmlClassLoaderBuilder(element, moduleSource, repository, versionManager).build();
    }

    private XmlClassLoaderBuilder(Element element, ModuleSource moduleSource, ArtifactRepository repository, VersionManager versionManager) {
        this.element = element;
        this.moduleSource = moduleSource;
        this.repository = repository;
        this.versionManager = versionManager;
    }

    private ClassLoadingConfig build() throws Exception {

        List<ClasspathEntry> classpath = new ArrayList<ClasspathEntry>();

        // First add module self
        ArtifactSharingMode selfSharingMode = ArtifactSharingMode.PROHIBITED;
        String selfShareModeName = element.getAttribute("share-self");
        if (selfShareModeName.length() > 0)
            selfSharingMode = ArtifactSharingMode.fromString(selfShareModeName);

        ClasspathEntry selfEntry = new ClasspathEntry(new FileArtifactRef(moduleSource.getClassPathEntry()), selfSharingMode, moduleSource);
        classpath.add(selfEntry);

        Element classPathElement = DocumentHelper.getElementChild(element, "classpath", false);
        if (classPathElement != null) {
            Element[] classPathEls = DocumentHelper.getElementChildren(classPathElement);
            classpath: for (Element classPathEl : classPathEls) {
                if (classPathEl.getLocalName().equals("artifact") && classPathEl.getNamespaceURI() == null) {
                    // Create ArtifactRef
                    String groupId = DocumentHelper.getAttribute(classPathEl, "groupId", true);
                    String artifactId = DocumentHelper.getAttribute(classPathEl, "artifactId", true);
                    String classifier = DocumentHelper.getAttribute(classPathEl, "classifier", false);
                    String version = DocumentHelper.getAttribute(classPathEl, "version", false);
                    String preferredVersion = versionManager.getPreferredVersion(groupId, artifactId);
                    version = version == null ? preferredVersion : version;
                    if (version == null) {
                        String message = String.format("Version for artifact %s:%s (%s) not specified, and no preference found in runtime configuration.", groupId, artifactId, classifier);
                        throw new RuntimeException(message);
                    }

                    ArtifactRef artifactRef = new RepoArtifactRef(groupId, artifactId, classifier, version);

                    // Check for double artifacts
                    for (ClasspathEntry entry : classpath) {
                        if (entry.getArtifactRef().equals(artifactRef)) {
                            log.error("Classloader specification contains second reference to same artifact, will skip second reference. Artifact = " + artifactRef);
                            continue classpath;
                        } else if (entry.getArtifactRef().getId().equals(artifactRef.getId())) {
                            log.warn("Classloader specification contains second reference to same artifact but different version. Artifact = " + artifactRef);
                        }
                    }

                    // Creating SharingMode
                    String sharingModeParam = classPathEl.getAttribute("share");
                    ArtifactSharingMode sharingMode;
                    if (sharingModeParam == null || sharingModeParam.equals(""))
                        sharingMode = ArtifactSharingMode.ALLOWED;
                    else
                        sharingMode = ArtifactSharingMode.fromString(sharingModeParam);

                    classpath.add(new ClasspathEntry(artifactRef, sharingMode, null));
                }
            }
        }

        return new ClassLoadingConfigImpl(classpath, repository);
    }
}
