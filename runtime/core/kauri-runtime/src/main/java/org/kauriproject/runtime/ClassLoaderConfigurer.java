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

import org.kauriproject.conf.Conf;
import org.kauriproject.runtime.module.ModuleConfig;
import org.kauriproject.runtime.rapi.ModuleSource;
import org.kauriproject.runtime.classloading.ClasspathEntry;
import org.kauriproject.runtime.classloading.ArtifactSharingMode;
import org.kauriproject.runtime.repository.ArtifactRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Implementation note: if multiple threads would concurrently use this class,
 * the log output of the different threads could produce meaningless interfered
 * results (if needed, can be easily fixed by outputting related things as one
 * log statement, but for current usage this is unneeded).
 */
public class ClassLoaderConfigurer {
    private List<ModuleConfig> moduleConfigs;
    private boolean enableSharing;
    private Map<String, ArtifactHolder> artifacts = new HashMap<String, ArtifactHolder>();
    private List<ClasspathEntry> sharedArtifacts = new ArrayList<ClasspathEntry>();

    private SharingConflictResolution requiredSharingConflictResolution = SharingConflictResolution.HIGHEST;
    private SharingConflictResolution allowedSharingConflictResolution = SharingConflictResolution.DONTSHARE;

    private final Log classLoadingLog = LogFactory.getLog(KauriRuntime.CLASSLOADING_LOG_CATEGORY);
    private final Log reportLog = LogFactory.getLog(KauriRuntime.CLASSLOADING_REPORT_CATEGORY);

    /**
     * Checks the class loader configurations of the modules (throws Exceptions in case of errors),
     * builds and returns a list of shareable artifacts, and adjust the class loader configurations
     * by marking the shareable artifacts.
     */
    public static List<ClasspathEntry> configureClassPaths(List<ModuleConfig> moduleConfigs, boolean enableSharing,
            Conf classLoadingConf) {
        return new ClassLoaderConfigurer(moduleConfigs, enableSharing, classLoadingConf).configure();
    }

    private ClassLoaderConfigurer(List<ModuleConfig> moduleConfigs, boolean enableSharing, Conf classLoadingConf) {
        this.moduleConfigs = moduleConfigs;
        this.enableSharing = enableSharing;
        
        Conf requiredConf = classLoadingConf.getChild("required", false);
        Conf allowedConf = classLoadingConf.getChild("allowed", false);
        
        if (requiredConf != null) {
            String requiredSharingStr = requiredConf.getAttribute("on-conflict", requiredSharingConflictResolution.getName());
            requiredSharingConflictResolution = SharingConflictResolution.fromString(requiredSharingStr);
            if (requiredSharingConflictResolution == null || requiredSharingConflictResolution.equals(SharingConflictResolution.DONTSHARE)) {
                throw new KauriRTException("Illegal value for required sharing conflict resolution (@on-conflict: " + requiredSharingStr, requiredConf.getLocation());
            }
        }
        
        if (allowedConf != null) {
            String allowedSharingStr = allowedConf.getAttribute("on-conflict", allowedSharingConflictResolution.getName());
            allowedSharingConflictResolution = SharingConflictResolution.fromString(allowedSharingStr);
            if (allowedSharingConflictResolution == null) {
                throw new KauriRTException("Illegal value for allowed sharing conflict resolution (@on-conflict:  " + allowedSharingStr, requiredConf.getLocation());
            }
        }
    }

    public List<ClasspathEntry> configure() {
        buildInverseIndex();
        handleArtifacts();
        logReport();
        return sharedArtifacts;
    }

    private void handleArtifacts() {
        for(ArtifactHolder holder : artifacts.values()) {
            handleArtifact(holder);
        }
    }

    private boolean handleArtifact(ArtifactHolder holder) {
        // If the artifact is share-required by some modules:
        //    - check everyone uses the same version
        //    - check that if other modules have this artifact as shared or private dependency, they are also all of the same version
        //    - put the artifact in the shared classloader and remove it from the individual module
        if (!handleRequired(holder)) {
            if (!handleProhibited(holder)){
                return handleAllowed(holder); 
            }
        }
        return true;
    }

    private boolean handleAllowed(ArtifactHolder holder) {
        if (holder.required.size() > 0 || holder.prohibited.size() > 0 || holder.allowed.size() == 0) {
            // nothing to do
            return false;
        }
        Set<String> versions = new HashSet<String>();
        for (ArtifactUser user : holder.allowed)
            versions.add(user.version);
        for (ArtifactUser user : holder.prohibited) {
            classLoadingLog.warn("Allowed-for-sharing artifact " + holder + " is also a prohibited-from-sharing dependency of " + user.module.getId());
            versions.add(user.version);
        }

        final boolean versionConflict = versions.size() > 1;
        
        if (!enableSharing) {
            classLoadingLog.info("Sharing of allowed artefacts is not enabled. Artifact " + holder + " will not be shared. It is used by " + holder.allowed.size() + " module(s).");
        } else if (!versionConflict) {
            if (holder.allowed.size() == 1) {
                classLoadingLog.info("Only one module uses the shareable artifact " + holder + ". It will not be added to the common classloader");
            } else {
                classLoadingLog.info("All modules using " + holder + " use the same version and allow sharing. Adding to the common classloader");
                makeShared(holder, versions.iterator().next());
            }
        } else if (allowedSharingConflictResolution.equals(SharingConflictResolution.HIGHEST)) {
            try {
                String version = getMostRecent(versions);
                makeShared(holder, version);
                classLoadingLog.info("Automatically used highest of multiple versions (" +
                        versionsToString(versions) + ") for shareable artifact " + holder + ": " + version);
            } catch (UncomparableVersionException e) {
                classLoadingLog.info("Multiple versions in use of shareable artifact " + holder +
                        ", and cannot compare them (" + versionsToString(versions) +
                        "), hence not adding it to the common classloader.");
            }
        } else if (allowedSharingConflictResolution.equals(SharingConflictResolution.DONTSHARE)) {
            classLoadingLog.info("Multiple versions in use of shareable artifact " + holder + ", hence not adding it to the common classloader.");
        } else {
            // if we get here, allowedSharingConflictResolution is SharingConflictResolution.ERROR
            classLoadingLog.error("Multiple modules use different versions of the share-allowed artifact " + holder);
            for (ArtifactUser user : holder.allowed) {
                classLoadingLog.error("  version " + user.version + " by " + user.module.getId() + " (sharing allowed)");
            }

            throw new KauriRTException("Multiple modules use different versions of the share-allowed artifact " + holder + ". Enable classloading logging to see details.");

        }
        return true;
    }

    private boolean handleProhibited(ArtifactHolder holder) {
        if (holder.required.size() > 0 || holder.prohibited.size() == 0) {
            return false;
        }
        Set<String> versions = new HashSet<String>();
        for (ArtifactUser user : holder.prohibited) versions.add(user.version);

        if (holder.allowed.size() > 1) {
            for (ArtifactUser user : holder.allowed) versions.add(user.version);
            
            classLoadingLog.info("Artifact sharing is allowed by some, but prohibited by:");
            for (ArtifactUser user : holder.prohibited)
                classLoadingLog.info("  " + user.module.getId());
        }
        
        if (versions.size() == 1) {
            // log info:
            classLoadingLog.info("Artifact sharing of " + holder + " is prohibited by some modules, but all use the same version. It might make sense to allow sharing the dependency.");
            for (ArtifactUser user : holder.prohibited)
                classLoadingLog.info("  " + user.module.getId());
        }
        return true;
    }

    private boolean handleRequired(ArtifactHolder holder) {
        if (holder.required.size() == 0) {
            // nothing to do 
            return false;
        }
        
        Set<String> versions = new HashSet<String>();
        for (ArtifactUser user : holder.required) {
            versions.add(user.version);
        }
        for (ArtifactUser user : holder.allowed) {
            versions.add(user.version);
        }
        for (ArtifactUser user : holder.prohibited) {
            versions.add(user.version);
            // we don't consider this a fatal error (for now)
            classLoadingLog.warn("Artifact required for sharing " + holder + " is also a prohibited from sharing by " + user.module.getId());
        }

        final boolean versionConflict = versions.size() > 1;
        final boolean handlingConflict = holder.prohibited.size() > 0;
        
        if (handlingConflict)
            classLoadingLog.warn("Sharing for artifact " + holder + " is both required and prohibited. Ignoring 'prohibited'.");
        if (versionConflict)
            classLoadingLog.warn("There are multiple versions for required-for sharing artifact " + holder + ". Using conflict resolution.");

        if (versionConflict && requiredSharingConflictResolution.equals(SharingConflictResolution.HIGHEST)) {
            try {
                String version = getMostRecent(versions);
                classLoadingLog.info("Automatically used highest of multiple versions (" +
                        versionsToString(versions) + ") for share-required artifact " + holder + ": " + version);
                makeShared(holder, version);
            } catch (UncomparableVersionException e) {
                throw new KauriRTException("Multiple modules use different versions of the share-required artifact " +
                        holder + " and cannot compare them: " + versionsToString(versions) +
                        ". Enable classloading logging to see details.");
            }
        } else if (versionConflict) {

            // if we get here, requiredSharingConflictResolution is SharingConflictResolution.ERROR
            classLoadingLog.error("Multiple modules use different versions of the share-required artifact " + holder);
            for (ArtifactUser user : holder.required) {
                classLoadingLog.error("  version " + user.version + " by " + user.module.getId() + " (sharing required)");
            }
            for (ArtifactUser user : holder.allowed) {
                classLoadingLog.error("  version " + user.version + " by " + user.module.getId() + " (sharing allowed)");
            }
            for (ArtifactUser user : holder.prohibited) {
                classLoadingLog.error("  version " + user.version + " by " + user.module.getId() + " (sharing prohibited)");
            }

            throw new KauriRTException("Multiple modules use different versions of the share-required artifact " + holder + ". Enable classloading logging to see details.");
        } else {
            classLoadingLog.info("Pushing " + holder + " with version " + versions.iterator().next() + " to the shared classloader.");
            makeShared(holder, versions.iterator().next());
        }
        return true;
    }

    private void makeShared(ArtifactHolder holder, String version) {
        ArtifactRef ref = holder.getArtifactRef(version);
        sharedArtifacts.add(new ClasspathEntry(ref, null, holder.getModuleSource()));

        for (ArtifactUser user : holder.required)
            user.module.getClassLoadingConfig().enableSharing(ref);
        for (ArtifactUser user : holder.allowed)
            user.module.getClassLoadingConfig().enableSharing(ref);
        for (ArtifactUser user : holder.prohibited)
            user.module.getClassLoadingConfig().enableSharing(ref);
    }

    private String versionsToString(Set<String> versions) {
        StringBuilder builder = new StringBuilder();
        for (String version : versions) {
            if (builder.length() > 0)
                builder.append(", ");
            builder.append(version);
        }
        return builder.toString();
    }

    /**
     * Builds an index of artifacts with pointers to the modules that use them
     * (= the inverse of the list of modules having the list of artifacts they use).
     */
    private void buildInverseIndex() {
        for (ModuleConfig moduleConf : moduleConfigs) {
            List<ClasspathEntry> classpathEntries = moduleConf.getClassLoadingConfig().getEntries();
            for (ClasspathEntry entry : classpathEntries) {
                ArtifactRef artifact = entry.getArtifactRef();
                ArtifactHolder holder = getArtifactHolder(artifact);
                holder.add(entry.getSharingMode(), artifact.getVersion(), moduleConf, entry.getModuleSource());
            }
        }
    }

    private ArtifactHolder getArtifactHolder(ArtifactRef artifact) {
        ArtifactHolder holder = artifacts.get(artifact.getId());
        if (holder == null) {
            holder = new ArtifactHolder(artifact);
            artifacts.put(artifact.getId(), holder);
        }
        return holder;
    }

    private void logReport() {
        if (!reportLog.isInfoEnabled())
            return;

        reportLog.info("Common classpath:");
        for (ClasspathEntry cpEntry : sharedArtifacts) {
            reportLog.info("  -> " + cpEntry.getArtifactRef().toString());
        }

        for (ModuleConfig moduleConf : moduleConfigs) {
            reportLog.info("Classpath of module " + moduleConf.getId());
            List<ClasspathEntry> cpEntries = moduleConf.getClassLoadingConfig().getUsedClassPath();

            if (artifacts.isEmpty()) {
                reportLog.info("  (empty)");
            } else {
                for (ClasspathEntry cpEntry : cpEntries) {
                    reportLog.info("  -> " + cpEntry.getArtifactRef().toString());
                }
            }
        }
    }

    private static class ArtifactHolder {
        String id;
        ArtifactRef artifactRef;
        ModuleSource moduleSource;
        List<ArtifactUser> required = new ArrayList<ArtifactUser>();
        List<ArtifactUser> allowed = new ArrayList<ArtifactUser>();
        List<ArtifactUser> prohibited = new ArrayList<ArtifactUser>();

        public ArtifactHolder(ArtifactRef artifactRef) {
            this.id = artifactRef.getId();
            this.artifactRef = artifactRef;
        }

        public void add(ArtifactSharingMode sharingMode, String version, ModuleConfig module, ModuleSource moduleSource) {
            switch (sharingMode) {
                case REQUIRED:
                    required.add(new ArtifactUser(version, module));
                    break;
                case ALLOWED:
                    allowed.add(new ArtifactUser(version, module));
                    break;
                case PROHIBITED:
                    prohibited.add(new ArtifactUser(version, module));
                    break;
            }

            if (this.moduleSource != null && this.moduleSource != moduleSource)
                throw new RuntimeException("Unexpected situation: two classpath entries based on same file location are both module-source based.");

            if (moduleSource != null)
                this.moduleSource = moduleSource;
        }

        public ArtifactRef getArtifactRef(String version) {
            return artifactRef.clone(version);
        }

        public ModuleSource getModuleSource() {
            return moduleSource;
        }

        public String toString() {
            return id;
        }
    }

    private static class ArtifactUser {
        String version;
        ModuleConfig module;

        public ArtifactUser(String version, ModuleConfig module) {
            this.version = version;
            this.module = module;
        }
    }

    /**
     *
     * @throws UncomparableVersionException if one the versions would not be something we can compare
     */
    public static String getMostRecent(Set<String> versions) {
        List<String> versionsList = new ArrayList<String>(versions);
        Collections.sort(versionsList, new VersionComparator());
        return versionsList.get(versionsList.size() - 1);
    }

    public static class VersionComparator implements Comparator<String> {
        public int compare(String o1, String o2) {
            return compareVersions(o1, o2);
        }
    }

    public static int compareVersions(String version1, String version2) throws UncomparableVersionException {
        // We assume versions follow the pattern: major.minor.revision-suffix
        Pattern pattern = Pattern.compile("(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:-(.*))?");

        Matcher m1 = pattern.matcher(version1);
        Matcher m2 = pattern.matcher(version2);

        if (!m1.matches())
            throw new UncomparableVersionException(version1);
        if (!m2.matches())
            throw new UncomparableVersionException(version2);

        int[] v1 = new int[3];
        for (int i = 0; i < 3; i++)
            v1[i] = m1.group(i + 1) == null ? 0 : Integer.parseInt(m1.group(i + 1));

        int[] v2 = new int[3];
        for (int i = 0; i < 3; i++)
            v2[i] = m2.group(i + 1) == null ? 0 : Integer.parseInt(m2.group(i + 1));

        for (int i = 0; i < 3; i++) {
            if (v1[i] > v2[i]) {
                return 1;
            } else if (v1[i] < v2[i]) {
                return -1;
            } else {
                // if equal, compare next part of the version
            }
        }

        // The dotted version parts are equal, now check the suffixes

        // A version without suffix is considered to be more recent than a version with suffix: the suffix serves
        // to indicate some snapshot version (-alpha, -r2323, ...) while the version without suffix is the final
        // release.

        String suffix1 = m1.group(4);
        String suffix2 = m2.group(4);

        if (suffix1 == null && suffix2 == null) {
            return 0;
        } else if (suffix1 == null) {
            return 1;
        } else if (suffix2 == null){
            return -1;
        }

        // Both have a suffix: try to compare

        Pattern revisionNumberPattern = Pattern.compile("r(\\d+)");

        Matcher rm1 = revisionNumberPattern.matcher(suffix1);
        Matcher rm2 = revisionNumberPattern.matcher(suffix2);

        if (!rm1.matches())
            throw new UncomparableVersionException(version1);
        if (!rm2.matches())
            throw new UncomparableVersionException(version2);

        Integer r1 = Integer.parseInt(rm1.group(1));
        Integer r2 = Integer.parseInt(rm2.group(1));

        return r1.compareTo(r2);
    }

    public static class UncomparableVersionException extends RuntimeException {
        public UncomparableVersionException(String version) {
            super("This version string is not comparable: " + version);
        }
    }
}
