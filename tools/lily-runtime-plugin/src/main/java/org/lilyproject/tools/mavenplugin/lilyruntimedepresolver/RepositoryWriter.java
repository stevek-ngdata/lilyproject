/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.tools.mavenplugin.lilyruntimedepresolver;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.plugin.MojoExecutionException;

public class RepositoryWriter {

    private RepositoryWriter() {
    }

    public static void write(Set<Artifact> artifacts, String targetDirectory) throws MojoExecutionException {
        for (Artifact artifact : artifacts) {
            File src = artifact.getFile();
            File dest = new File(targetDirectory, pathOf(artifact));
            try {
                FileUtils.copyFile(src, dest);
            } catch (IOException e) {
                throw new MojoExecutionException("Error copying file " + src + " to " + dest);
            }

            // Lily Runtime does not need the pom files, but let's copy them anyway, for informational purposes
            File srcPom = pomFile(src);
            File destPom = pomFile(dest);
            if (srcPom != null && srcPom.exists()) {
                try {
                    FileUtils.copyFile(srcPom, destPom);
                } catch (IOException e) {
                    throw new MojoExecutionException("Error copying file " + srcPom + " to " + destPom);
                }
            }
        }
    }

    public static File pomFile(File jarFile) throws MojoExecutionException {
        String path = jarFile.getAbsolutePath();

        if (!path.endsWith(".jar")) {
            return null;
        }

        return new File(path.replaceAll("\\.jar$", ".pom"));
    }


    private static final char PATH_SEPARATOR = '/';
    private static final char GROUP_SEPARATOR = '.';
    private static final char ARTIFACT_SEPARATOR = '-';

    /**
     * disclaimer: this method has been copied from the DefaultRepositoryLayout of the Maven
     * source tree and is Apache licensed. It was changed to use getBaseVersion() instead
     * of getVersion() on the artifact.
     */
    private static String pathOf(Artifact artifact) {
        ArtifactHandler artifactHandler = artifact.getArtifactHandler();

        StringBuilder path = new StringBuilder();

        path.append(formatAsDirectory(artifact.getGroupId())).append(PATH_SEPARATOR);
        path.append(artifact.getArtifactId()).append(PATH_SEPARATOR);
        path.append(artifact.getBaseVersion()).append(PATH_SEPARATOR);
        // Lily change: Here we call getBaseVersion() instead of getVersion() because otherwise for snapshot artifacts
        // a timestamp suffix is included in the version (and our shell scripts & runtime classloader.xml's
        // refer to the plain snapshot version)
        path.append(artifact.getArtifactId()).append(ARTIFACT_SEPARATOR).append(artifact.getBaseVersion());

        if (artifact.hasClassifier()) {
            path.append(ARTIFACT_SEPARATOR).append(artifact.getClassifier());
        }

        if (artifactHandler.getExtension() != null && artifactHandler.getExtension().length() > 0) {
            path.append(GROUP_SEPARATOR).append(artifactHandler.getExtension());
        }

        return path.toString();
    }

    private static String formatAsDirectory( String directory ) {
        return directory.replace(GROUP_SEPARATOR, PATH_SEPARATOR);
    }
}
