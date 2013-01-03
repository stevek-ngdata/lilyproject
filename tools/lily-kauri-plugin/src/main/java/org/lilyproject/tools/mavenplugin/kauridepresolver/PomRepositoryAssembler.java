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
package org.lilyproject.tools.mavenplugin.kauridepresolver;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.RepositorySystemSession;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.collection.DependencyCollectionException;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.DependencyNode;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.resolution.DependencyRequest;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.filter.ScopeDependencyFilter;
import org.sonatype.aether.util.graph.PreorderNodeListGenerator;

/**
 * @goal assemble-pom-repository
 * @requiresDependencyResolution runtime
 * @description Resolve (download) a list of artifacts and their (transitive) runtime dependencies to a m2 repository layout dir.
 */
public class PomRepositoryAssembler extends AbstractMojo {

    /**
     * The entry point to Aether, i.e. the component doing all the work.
     *
     * @component
     */
    private RepositorySystem repoSystem;

    /**
     * The current repository/network configuration of Maven.
     *
     * @parameter default-value="${repositorySystemSession}"
     * @readonly
     */
    private RepositorySystemSession repoSession;

    /**
     * The project's remote repositories to use for the resolution of plugins and their dependencies.
     *
     * @parameter default-value="${project.remotePluginRepositories}"
     * @readonly
     */
    private List<RemoteRepository> remoteRepos;

    /**
     * @parameter
     */
    private List<String> artifacts;

    /**
     * @parameter default-value="${basedir}/target/kauri-repository"
     * @parameter
     */
    private String targetDirectory;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        List<Artifact> result = Lists.newArrayList();
        for (String artifact : artifacts) {
            result.addAll(getArtifacts(artifact));
        }

        Set<Artifact> artifacts = new HashSet<Artifact>(result);
        AetherRepositoryWriter.write(artifacts, targetDirectory);

    }

    private List<Artifact> getArtifacts(String artifact) throws MojoExecutionException {
        Dependency dependency =
                new Dependency(new DefaultArtifact(artifact), "runtime");

        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(dependency);
        //collectRequest.addRepository( remoteRepos );

        DependencyNode node;
        try {
            node = repoSystem.collectDependencies(repoSession, collectRequest).getRoot();
        } catch (DependencyCollectionException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        DependencyRequest dependencyRequest = new DependencyRequest(node, null);
        Set<String> included = Collections.singleton("runtime");
        dependencyRequest.setFilter(new ScopeDependencyFilter(included, Collections.EMPTY_SET));

        try {
            repoSystem.resolveDependencies(repoSession, dependencyRequest);
        } catch (DependencyResolutionException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        PreorderNodeListGenerator nlg = new PreorderNodeListGenerator();
        node.accept(nlg);
        getLog().info("" + nlg.getClassPath());

        return nlg.getArtifacts(false);
    }

    private List<Dependency> collectDependencies(DependencyNode node) {
        List<Dependency> result = Lists.newArrayList();
        collectDependencies(node, result);
        return result;
    }

    private void collectDependencies(DependencyNode node, List<Dependency> result) {
        result.add(node.getDependency());
        for (DependencyNode child : node.getChildren()) {
            collectDependencies(child);
        }
    }

}
