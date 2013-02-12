/*
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
package org.kauriproject.tools.deployrepo;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

public class DeployRepo {
    public static void main(String[] args) throws Exception {
        new DeployRepo().run();
    }

    public void run() throws Exception {
        File mavenRepo = findMavenRepo();
        if (!mavenRepo.exists()) {
            System.out.println("Your local Maven repository does not yet exist at:");
            System.out.println("  " + mavenRepo);
            System.out.println("It will be created.");
            mavenRepo.mkdirs();
            if (!mavenRepo.exists()) {
                System.out.println("Failed to create the local Maven repository directory.");
                System.exit(1);
            }
        } else {
            System.out.println("Found your local Maven repository at: ");
            System.out.println("  " + mavenRepo.getAbsolutePath());
        }

        String kauriHome = System.getProperty("kauri.home");
        File kauriRepo = new File(kauriHome + "/lib");
        if (!kauriRepo.exists()) {
            System.out.println("Could not find Kauri's Maven repository at:");
            System.out.println(kauriRepo);
        }

        System.out.println();
        System.out.println("Coping Kauri repository from");
        System.out.println("  " + kauriRepo.getAbsolutePath());
        System.out.println("to");
        System.out.println("  " + mavenRepo.getAbsolutePath());
        System.out.println();
        copy(kauriRepo, mavenRepo, "");
        System.out.println("Done.");
    }

    private File findMavenRepo() throws Exception {
        String userHome = System.getProperty("user.home");
        File mavenSettingsFile = new File(userHome + "/.m2/settings.xml");
        if (mavenSettingsFile.exists()) {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(false);
            Document settingsDoc;
            try {
                settingsDoc = dbf.newDocumentBuilder().parse(mavenSettingsFile);
            } catch (Throwable t) {
                throw new Exception("Error reading your Maven settings file from " + mavenSettingsFile.getAbsolutePath());
            }

            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            String localRepo = (String)xpath.evaluate("/settings/localRepository", settingsDoc, XPathConstants.STRING);
            if (localRepo != null && localRepo.trim().length() > 0) {
                return new File(localRepo);
            }
        }

        return new File(userHome + "/.m2/repository");
    }

    private void copy(File fromDir, File toDir, String fromPath) {
        File[] files = fromDir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                String newFromPath = fromPath + "/" + file.getName();
                File destDir = new File(toDir, newFromPath);
                destDir.mkdirs();
                if (!destDir.exists()) {
                    System.err.println("Failed to created directory at:");
                    System.err.println("  " + destDir.getAbsolutePath());
                    System.exit(1);
                }
                copy(file, destDir, fromPath);
            } else {
                if (!file.getName().equals("archetype-catalog.xml") && !file.getName().startsWith("maven-metadata"))
                copyFile(file, new File(toDir, file.getName()));
            }
        }
    }

    private void copyFile(File fromFile, File toFile) {
        System.out.print('.');
        try {
            FileInputStream srcFis = new FileInputStream(fromFile);
            FileOutputStream destFos = new FileOutputStream(toFile);
            FileChannel srcChannel = srcFis.getChannel();
            FileChannel dstChannel = destFos.getChannel();
            dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
            srcChannel.close();
            dstChannel.close();
            srcFis.close();
            destFos.close();
        } catch (Throwable t) {
            System.err.println("Failed to copy file from");
            System.err.println("  " + fromFile.getAbsolutePath());
            System.err.println("to");
            System.err.println("  " + toFile.getAbsolutePath());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
