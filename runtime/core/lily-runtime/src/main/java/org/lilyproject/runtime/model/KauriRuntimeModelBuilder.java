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
package org.lilyproject.runtime.model;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.runtime.conf.XmlConfBuilder;
import org.lilyproject.runtime.KauriRTException;
import org.lilyproject.runtime.KauriRuntime;
import org.lilyproject.runtime.repository.ArtifactRepository;

public class KauriRuntimeModelBuilder {
    public static KauriRuntimeModel build(File runtimeConfig, Set<String> disabledModuleIds,
            ArtifactRepository repository) throws Exception {
        Conf conf = XmlConfBuilder.build(runtimeConfig);
        return build(conf, disabledModuleIds, repository, new SourceLocations());
    }

    public static KauriRuntimeModel build(File runtimeConfig, Set<String> disabledModuleIds,
            ArtifactRepository repository, SourceLocations artifactSourceLocations) throws Exception {
        Conf conf = XmlConfBuilder.build(runtimeConfig);
        return build(conf, disabledModuleIds, repository, artifactSourceLocations);
    }

    public static KauriRuntimeModel build(Conf runtimeConf, Set<String> disabledModuleIds,
            ArtifactRepository repository, SourceLocations artifactSourceLocations) throws Exception {

        KauriRuntimeModel model = new KauriRuntimeModel();

        if (runtimeConf.getChild("connectors", false) != null) {
            LogFactory.getLog(KauriRuntimeModelBuilder.class).error("Found a <connectors> child element in your "
                    + "modules configuration. This will be unused, from Kauri 0.4 on the connectors configuration "
                    + "has moved to a different file.");
        }

        Stack<String> wiringFileStack = new Stack<String>(); // used to detect recursive wiring.xml inclusions
        List<ModuleDefinition> modules = new ArrayList<ModuleDefinition>();

        buildModules(modules, wiringFileStack, runtimeConf, repository, artifactSourceLocations);

        // Remove disabled module entries
        Iterator<ModuleDefinition> it = modules.iterator();
        while (it.hasNext()) {
            ModuleDefinition entry = it.next();
            if (disabledModuleIds.contains(entry.getId()))
                it.remove();
        }

        for (ModuleDefinition md : modules)
            model.addModule(md);

        return model;
    }

    private static void buildModules(List<ModuleDefinition> modules, Stack<String> wiringFileStack, Conf runtimeConf,
            ArtifactRepository repository, SourceLocations artifactSourceLocations) throws Exception {

        Conf modulesConf = runtimeConf.getRequiredChild("modules");
        List<Conf> importConfs = modulesConf.getChildren();
        for (Conf importConf : importConfs) {
            File fileToImport = null;
            ModuleSourceType sourceType = null;
            String version = "unknown";
            String id = null;
            if (importConf.getName().equals("file")) {
                String path = PropertyResolver.resolveProperties(importConf.getAttribute("path"));
                File file = new File(path);
                if (file.getName().toLowerCase().endsWith(".xml")) {
                    // Import a wiring.xml-type file
                    includeWiring(file, modules, wiringFileStack, repository, artifactSourceLocations);
                } else {
                    id = importConf.getAttribute("id");
                    fileToImport = new File(path);
                    sourceType = ModuleSourceType.JAR;
                }
            } else if (importConf.getName().equals("artifact")) {
                id = importConf.getAttribute("id");
                String groupId = importConf.getAttribute("groupId");
                String artifactId = importConf.getAttribute("artifactId");
                String classifier = importConf.getAttribute("classifier", null);
                // Version is optional for org.lilyproject artifacts
                version = groupId.equals("org.lilyproject") ?
                        importConf.getAttribute("version", null) : importConf.getAttribute("version");
                version = version == null ? KauriRuntime.getVersion() : version;

                File sourceLocation = artifactSourceLocations.getSourceLocation(groupId, artifactId);
                if (sourceLocation != null) {
                    fileToImport = sourceLocation.getCanonicalFile();
                    if (!fileToImport.exists())
                        throw new KauriRTException("Specified artifact source directory does not exist: "
                                + fileToImport.getAbsolutePath(), importConf.getLocation());
                    sourceType = ModuleSourceType.SOURCE_DIRECTORY;
                } else {
                    fileToImport = repository.resolve(groupId, artifactId, classifier, version);
                    sourceType = ModuleSourceType.JAR;
                }
            } else if (importConf.getName().equals("directory")) {
                id = importConf.getAttribute("id");

                String dirName = PropertyResolver.resolveProperties(importConf.getAttribute("path"));

                // When basePath is specified, the directory specified in dirName will be resolved
                // against basePath. Moreover, basePath can contain a list of paths,
                // in which case each basePath will be combined with the path. (this is the only
                // reason for having basePath as a separate attribute)
                // (It would make sense to allow multiple values for path as well, but the
                // need hasn't come up)
                String basePath = importConf.getAttribute("basePath", null);
                if (basePath != null) {
                    basePath = PropertyResolver.resolveProperties(basePath);
                    String[] basePaths = basePath.split(File.pathSeparator);
                    for (String path : basePaths) {
                        path = path.trim();
                        if (path.length() > 0) {
                            String subDirName = new File(new File(path), dirName).getAbsolutePath();
                            include(subDirName, id, modules, wiringFileStack, repository, artifactSourceLocations);
                        }
                    }
                } else {
                    include(dirName, id, modules, wiringFileStack, repository, artifactSourceLocations);
                }

            } else {
                throw new KauriRTException("Unexpected node: " + importConf.getName(), importConf.getLocation());
            }

            if (fileToImport != null) {
                if (!fileToImport.exists()) {
                    throw new KauriRTException("Import does not exist: " + fileToImport.getAbsolutePath(),
                            importConf.getLocation());
                }

                ModuleDefinition moduleDefinition = new ModuleDefinition(id, fileToImport, sourceType);
                moduleDefinition.setLocation(importConf.getLocation());
                moduleDefinition.setVersion(version);
                buildWiring(importConf, moduleDefinition);

                modules.add(moduleDefinition);
            }
        }
    }

    private static void include(String dirName, String id, List<ModuleDefinition> modules,
                                Stack<String> wiringFileStack, ArtifactRepository repository,
                                SourceLocations artifactSourceLocations) throws Exception {
        LogFactory.getLog(KauriRuntimeModelBuilder.class).debug("Searching for wiring includes at " + dirName);
        File dir = new File(dirName);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            // order of imports is important, to provide some deterministic behaviour, sort the entries by name
            Arrays.sort(files, new FileNameComparator());
            for (File file : files) {
                if (file.isDirectory()) {
                    // skip
                } else if (file.getName().toLowerCase().endsWith(".jar")) {
                    String genId = id + "-" + stripJarExt(file.getName());
                    modules.add(new ModuleDefinition(genId, file, ModuleSourceType.JAR));
                } else if (file.getName().toLowerCase().endsWith(".xml")) {
                    // Import a wiring.xml-type file
                    includeWiring(file, modules, wiringFileStack, repository, artifactSourceLocations);
                }
            }
        }
    }

    private static void includeWiring(File file, List<ModuleDefinition> modules, Stack<String> wiringFileStack,
            ArtifactRepository repository, SourceLocations artifactSourceLocations) throws Exception {

        String key = file.getCanonicalPath();

        if (wiringFileStack.contains(key)) {
            throw new KauriRTException("Recursive loading of wiring.xml-type file detected: " + key);
        }

        Conf conf = XmlConfBuilder.build(file);

        // go recursive
        wiringFileStack.push(key);
        buildModules(modules, wiringFileStack, conf, repository, artifactSourceLocations);
        wiringFileStack.pop();
    }

    private static Pattern URL_REF_PATTERN = Pattern.compile("^url\\(([^)]*)\\)$");
    private static Pattern MODULE_REF_PATTERN = Pattern.compile("^([^:]*):([^:]*)$");

    public static void buildWiring(Conf conf, ModuleDefinition moduleDef) {
        List<Conf> children = conf.getChildren();
        for (Conf child : children) {
            if (child.getName().equals("inject-javaservice")) {
                String name = child.getAttribute("name", null);
                String service = child.getAttribute("service", null);
                if (name == null && service == null)
                    throw new KauriRTException("Either name or service attribute should be specified on inject-javaservice", child.getLocation());
                String ref = child.getAttribute("ref");

                String sourceModule;
                String sourceName = null;
                Matcher matcher = MODULE_REF_PATTERN.matcher(ref);
                if (matcher.matches()) {
                    sourceModule = matcher.group(1);
                    sourceName = matcher.group(2);
                } else {
                    sourceModule = ref;
                }

                if (name != null) {
                    moduleDef.addInject(new JavaServiceInjectByNameDefinition(name, sourceModule, sourceName));
                } else {
                    moduleDef.addInject(new JavaServiceInjectByServiceDefinition(service, sourceModule, sourceName));
                }
            }
        }
    }

    private static String stripJarExt(String name) {
        if (name.endsWith(".jar")) {
            return name.substring(0, name.length() - 4);
        } else {
            return name;
        }
    }

    private static class FileNameComparator implements Comparator<File> {
        public int compare(File o1, File o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }
}
