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
package org.lilyproject.lilyservertestfwloader;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.w3c.dom.Document;

/**
 * Run the LilyLauncher with a classloader configured based on a classloader.xml file. This is done to prevent
 * unwieldy long classpath environment variables to be setup in the launcher scripts.
 */
public class ClassLoadingLilyLauncher {
    private File repositoryLocation;

    public static void main(String[] args) throws Throwable {
        File repositoryLocation;
        String param = System.getProperty("lily.testlauncher.repository");
        if (param != null) {
            repositoryLocation = new File(param);
        } else {
            repositoryLocation = findLocalMavenRepository();
        }

        launch(repositoryLocation, args);
    }

    public static void launch(File repositoryLocation, String[] args) throws Throwable {
        new ClassLoadingLilyLauncher(repositoryLocation).run(args);
    }

    private ClassLoadingLilyLauncher(File repositoryLocation) {
        this.repositoryLocation = repositoryLocation;
    }


    public void run(String[] args) throws Throwable {

        ClassLoader classLoader = LauncherClasspathHelper.getClassLoader(
                "org/lilyproject/lilyservertestfwloader/classloader.xml", repositoryLocation, buildAdditionalClassPath());
        Thread.currentThread().setContextClassLoader(classLoader);

        Method mainMethod;
        try {
            Class runtimeClass = classLoader.loadClass("org.lilyproject.lilyservertestfw.launcher.LilyLauncher");
            mainMethod = runtimeClass.getMethod("main", String[].class);
        } catch (Exception e) {
            throw new RuntimeException("Error loading Lily launcher", e);
        }

        try {
            mainMethod.invoke(null, (Object) args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error launching Lily launcher", e);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private List<URL> buildAdditionalClassPath() {
        String additionalCp = System.getProperty("lily.testlauncher.additional.classpath");
        if (additionalCp == null) {
            return Collections.emptyList();
        } else {
            Iterable<String> strings = Splitter.on(":").trimResults().split(additionalCp);
            return Lists.newArrayList(Iterables.transform(strings, new Function<String, URL>() {
                @Override
                public URL apply(String input) {
                    try {
                        return new File(input).toURI().toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("error parsing additional classpath", e);
                    }
                }
            }));
        }
    }

    private static File findLocalMavenRepository() {
        String homeDir = System.getProperty("user.home");
        File mavenSettingsFile = new File(homeDir + "/.m2/settings.xml");
        if (mavenSettingsFile.exists()) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setNamespaceAware(true);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document document = db.parse(mavenSettingsFile);
                XPath xpath = XPathFactory.newInstance().newXPath();
                SimpleNamespaceContext nc = new SimpleNamespaceContext();
                nc.addPrefix("m", "http://maven.apache.org/POM/4.0.0");
                xpath.setNamespaceContext(nc);

                String localRepository = xpath.evaluate("string(/m:settings/m:localRepository)", document);
                if (localRepository != null && localRepository.length() > 0) {
                    return new File(localRepository);
                }

                // Usage of the POM namespace in settings.xml is optional, so also try without namespace
                localRepository = xpath.evaluate("string(/settings/localRepository)", document);
                if (localRepository != null && localRepository.length() > 0) {
                    return new File(localRepository);
                }
            } catch (Exception e) {
                System.err.println("Error reading Maven settings file at " + mavenSettingsFile.getAbsolutePath());
                e.printStackTrace();
                System.exit(1);
            }
        }
        return new File(homeDir + "/.m2/repository");
    }

    public static class SimpleNamespaceContext implements NamespaceContext {
        private Map<String, String> prefixToUri = new HashMap<String, String>();

        public void addPrefix(String prefix, String uri) {
            prefixToUri.put(prefix, uri);
        }

        @Override
        public String getNamespaceURI(String prefix) {
            if (prefix == null) {
                throw new IllegalArgumentException("Null argument: prefix");
            }

            if (prefix.equals(XMLConstants.XML_NS_PREFIX)) {
                return XMLConstants.XML_NS_URI;
            } else if (prefix.equals(XMLConstants.XMLNS_ATTRIBUTE)) {
                return XMLConstants.XMLNS_ATTRIBUTE_NS_URI;
            }

            String uri = prefixToUri.get(prefix);
            if (uri != null) {
                return uri;
            } else {
                return XMLConstants.NULL_NS_URI;
            }
        }

        @Override
        public String getPrefix(String namespaceURI) {
            throw new RuntimeException("Not implemented.");
        }

        @Override
        public Iterator getPrefixes(String namespaceURI) {
            throw new RuntimeException("Not implemented.");
        }
    }
}
