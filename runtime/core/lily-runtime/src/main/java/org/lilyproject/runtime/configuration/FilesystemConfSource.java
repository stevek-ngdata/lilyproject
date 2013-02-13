package org.lilyproject.runtime.configuration;

import java.io.*;
import java.util.*;

public class FilesystemConfSource extends ConfSource {
    private File baseDir;

    public FilesystemConfSource(File baseDir) {
        super(baseDir.getAbsolutePath());
        this.baseDir = baseDir;
    }

    protected List<ConfigPath> getConfigFiles() {
        List<ConfigPath> configPaths = new ArrayList<ConfigPath>();
        collectConfigFiles(baseDir, configPaths, baseDir);
        return configPaths;
    }

    private void collectConfigFiles(File dir, List<ConfigPath> configPaths, File rootDir) {
        File[] files = dir.listFiles(CONFIG_FILE_FILTER);
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    collectConfigFiles(file, configPaths, rootDir);
                } else {
                    String path = getConfigPathForFile(file, rootDir);
                    if (path.endsWith(CONFIG_FILE_EXT)) { // should be the case
                        path = path.substring(0, path.length() - CONFIG_FILE_EXT.length());
                    }
                    configPaths.add(new ConfigPath(path, new FilesystemConfigFile(file)));
                }
            }
        }
    }

    private String getConfigPathForFile(File file, File reference) {
        File parent = file.getParentFile();
        if (parent != null && !parent.equals(reference)) {
            return getConfigPathForFile(parent, reference) + "/" + file.getName();
        } else if (parent != null) {
            return file.getName();
        } else {
            return "";
        }
    }

    private static final FileFilter CONFIG_FILE_FILTER = new FileFilter() {
        public boolean accept(File pathname) {
            return acceptFileName(pathname.isDirectory(), pathname.getName());
        }
    };

    private static class FilesystemConfigFile implements ConfigFile {
        private File file;

        public FilesystemConfigFile(File file) {
            this.file = file;
        }

        public InputStream getInputStream() throws IOException {
            return new BufferedInputStream(new FileInputStream(file));
        }

        public String getPath() {
            return file.getAbsolutePath();
        }

        public long lastModified() {
            return file.lastModified();
        }
    }
}
