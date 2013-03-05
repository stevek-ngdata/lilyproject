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
package org.lilyproject.runtime.model;

import java.util.Properties;
import java.util.Map;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;

/**
 * Mapping from artifact identifiers to the corresponding source tree location.
 */
public class SourceLocations {
    private Properties locations;
    private File baseDir;

    public SourceLocations() {
        locations = new Properties();
        baseDir = new File(System.getProperty("user.dir"));
    }

    public SourceLocations(InputStream is, String moduleSourceLocationsDir) throws IOException {
        baseDir = new File(moduleSourceLocationsDir);
        load(is);
    }

    public File getSourceLocation(String groupId, String artifactId) {
        String location = locations.getProperty(groupId + "!" + artifactId);
        if (location != null) {
            File file = new File(location);
            if (!file.isAbsolute()) {
                // Paths are relative to the location of the 'module source locations' file.
                file = new File(baseDir, location);
            }
            return file;
        } else {
            return null;
        }
    }

    private void load(InputStream is) throws IOException {
        locations = new Properties();
        locations.load(is);

        Properties properties = System.getProperties();

        for (Map.Entry entry : locations.entrySet()) {
            String value = (String)entry.getValue();
            String resolvedValue = PropertyResolver.resolveProperties(value, properties);
            entry.setValue(resolvedValue);
        }
    }
}
