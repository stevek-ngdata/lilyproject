/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.lilyservertestfw;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class ConfUtil {
    public static final String CONF_RESOURCE_PATH = "org/lilyproject/lilyservertestfw/conf/";

    public static void copyConfResources(URL confUrl, String confResourcePath, File confDir) throws IOException, URISyntaxException {
        URLConnection urlConnection = confUrl.openConnection();
        if (urlConnection instanceof JarURLConnection) {
            copyFromJar(confDir, confResourcePath, (JarURLConnection)urlConnection);
        } else {
            FileUtils.copyDirectory(new File(confUrl.toURI()), confDir);
        }
    }

    public static void copyFromJar(File confDir, String confResourcePath, JarURLConnection jarConnection) throws IOException {
        JarFile jarFile = jarConnection.getJarFile();
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if (entry.getName().startsWith(confResourcePath)) {
                String fileName = StringUtils.removeStart(entry.getName(), confResourcePath);
                if (entry.isDirectory()) {
                    File subDir = new File(confDir, fileName);
                    subDir.mkdirs();
                } else {
                    InputStream entryInputStream = null;
                    try {
                        entryInputStream = jarFile.getInputStream(entry);
                        FileUtils.copyInputStreamToFile(entryInputStream, new File(confDir, fileName));
                    } finally {
                        if (entryInputStream != null) {
                            entryInputStream.close();
                        }
                  }
                }
            }
        }
    }
}
