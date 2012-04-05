package org.lilyproject.lilyservertestfw;

import org.apache.commons.io.FileUtils;
import org.lilyproject.util.Version;
import org.lilyproject.util.test.TestHomeUtil;

import java.io.File;
import java.io.IOException;

public class TemplateDir {
    public static String HADOOP_DIR = "hadoop";
    public static String SOLR_DIR = "solr";
    public static String LILYSERVER_DIR = "lilyserver";

    public static void restoreTemplateDir(File testHome) throws IOException {
        File templateDir = getTemplateDir();
        if (templateDir.exists()) {
            System.out.println("----------------------------------------------------------");
            System.out.println("Restoring template data directory");
            System.out.println(templateDir.getAbsolutePath());
            System.out.println("----------------------------------------------------------");
            FileUtils.copyDirectory(templateDir, testHome);
        } else {
            System.out.println("----------------------------------------------------------");
            System.out.println("Tip: for faster startup in the future, run once:");
            System.out.println("launch-test-lily --prepare");
            System.out.println("----------------------------------------------------------");
        }
    }

    public static void makeTemplateDir(File testHome) throws IOException {
        File destination = getTemplateDir();

        if (destination.exists()) {
            System.out.println("Removing existing directory " + destination.getAbsolutePath());
            FileUtils.deleteDirectory(destination);
        }

        System.out.println("Copying data directory state to " + destination.getAbsolutePath());
        FileUtils.copyDirectory(testHome, destination);

        System.out.println("Deleting data directory " + testHome.getAbsolutePath());
        TestHomeUtil.cleanupTestHome(testHome);
    }

    public static void deleteTemplateDir() throws IOException {
        File destination = getTemplateDir();
        System.out.println("Removing existing directory " + destination.getAbsolutePath());
        FileUtils.deleteDirectory(destination);
    }

    public static File getTemplateDir() {
        String version = Version.readVersion("org.lilyproject", "lily-server-test-fw");
        return new File(System.getProperty("user.home") + "/.lily/launcher/template/" + version);
    }
}
