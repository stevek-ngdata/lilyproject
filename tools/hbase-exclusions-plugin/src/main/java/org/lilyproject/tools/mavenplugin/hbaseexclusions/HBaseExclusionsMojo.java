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
package org.lilyproject.tools.mavenplugin.hbaseexclusions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;

/**
 * This is a temporary Maven plugin used in the lily-hbase-client project to check that there are no
 * redundant dependencies. Otherwise prints out the necessary exclusion-statements and fails the build.
 * <p/>
 * <p>This will be invalidated once there is a solution for this issue:
 * https://issues.apache.org/jira/browse/HBASE-2170
 *
 * @requiresDependencyResolution runtime
 * @goal generate-exclusions
 */
public class HBaseExclusionsMojo extends AbstractMojo {
    /**
     * @parameter expression="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        List<Artifact> dependencies = (List<Artifact>)project.getRuntimeArtifacts();
        // excludes grouped by parent artifact
        Map<String, List<Artifact>> excludesByParent = new HashMap<String, List<Artifact>>();

        for (Artifact artifact : dependencies) {
            // Rather than simply outputting all dependencies as excludes, we only want to output
            // an exclude for the direct children of the allowed artifact, because the other ones
            // will be disabled recursively by Maven.
            int allowedParentPos = getAllowedParentPostion(artifact.getDependencyTrail());
            if (allowedParentPos != -1 && allowedParentPos == artifact.getDependencyTrail().size() - 2) {
                // entry at level 0 in dependency trail is the project itself, entry at position 1 the
                // dependencies listed in the pom, and deeper are the recursive deps
                String topLevelParent = (String)artifact.getDependencyTrail().get(1);
                List<Artifact> excludes = excludesByParent.get(topLevelParent);
                if (excludes == null) {
                    excludes = new ArrayList<Artifact>();
                    excludesByParent.put(topLevelParent, excludes);
                }
                excludes.add(artifact);
            }
        }

        if (excludesByParent.size() > 0) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("Please add these excludes to the lily-hbase-client pom:");
            System.out.println();
            System.out.println();
            for (Map.Entry<String, List<Artifact>> entry : excludesByParent.entrySet()) {
                System.out.println("<dependency>");
                String[] artifactParts = parseArtifactName(entry.getKey());
                System.out.println("  <groupId>" + artifactParts[0] + "</groupId>");
                System.out.println("  <artifactId>" + artifactParts[1] + "</artifactId>");
                System.out.println("  <exclusions>");
                for (Artifact artifact : entry.getValue()) {
                    System.out.println("    <exclusion>");
                    System.out.println("      <groupId>" + artifact.getGroupId() + "</groupId>");
                    System.out.println("      <artifactId>" + artifact.getArtifactId() + "</artifactId>");
                    System.out.println("    </exclusion>");
                }
                System.out.println("  <exclusions>");
                System.out.println("</dependency>");
            }
            System.out.println();
            System.out.println();
            System.out.println();

            throw new MojoExecutionException("lily-hbase-client is missing some excludes, please adjust");
        }
    }

    private int getAllowedParentPostion(List<String> trail) {
        for (int i = trail.size() - 1; i >= 0; i--) {
            String artifact = trail.get(i);
            String[] artifactParts = parseArtifactName(artifact);
            if (isAllowed(artifactParts[0], artifactParts[1])) {
                return i;
            }
        }
        return -1;
    }

    private String[] parseArtifactName(String artifact) {
        int groupIdEnd = artifact.indexOf(':');
        String groupId = artifact.substring(0, groupIdEnd);
        int artifactIdEnd = artifact.indexOf(':', groupIdEnd + 1);
        String artifactId = artifact.substring(groupIdEnd + 1, artifactIdEnd);

        return new String[]{groupId, artifactId};
    }

    private static Set<String> ALLOWED_ARTIFACTS = new HashSet<String>();

    static {
        ALLOWED_ARTIFACTS.add("org.apache.hbase:hbase");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:zookeeper");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:hadoop-core");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:hadoop-common");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:hadoop-hdfs");
        ALLOWED_ARTIFACTS.add("com.google.guava:guava");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop.thirdparty.guava:guava");
        ALLOWED_ARTIFACTS.add("org.apache.zookeeper:zookeeper");
        ALLOWED_ARTIFACTS.add("commons-configuration:commons-configuration");
        ALLOWED_ARTIFACTS.add("org.apache.hadoop:hadoop-auth");
        ALLOWED_ARTIFACTS.add("com.google.protobuf:protobuf-java");
    }

    private boolean isAllowed(String groupId, String artifactId) {
        return ALLOWED_ARTIFACTS.contains(groupId + ":" + artifactId);
    }
}
