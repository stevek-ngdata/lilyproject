package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.lilyproject.lilyservertestfw.LilyServerTestUtility;

import java.io.File;
import java.net.URL;
import java.util.List;

public class LilyLauncherService implements LauncherService {
    private LilyServerTestUtility lilyServerTestUtility;
    private File testHome;

    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome) throws Exception {
        this.testHome = testHome;
        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        // TODO need to make up mind about what conf dirs to use, for now just using the built-in default conf
        File confDir = new File(testHome, "lilyconf");
        FileUtils.forceMkdir(confDir);

        URL confUrl = getClass().getClassLoader().getResource("org/lilyproject/lilyservertestfw/conf/");
        FileUtils.copyDirectory(new File(confUrl.toURI()), confDir);

        lilyServerTestUtility = new LilyServerTestUtility(confDir.getAbsolutePath());
        lilyServerTestUtility.start();

        postStartupInfo.add("-----------------------------------------------");
        postStartupInfo.add("Lily is running");
        postStartupInfo.add("");
        postStartupInfo.add("Using configuration from: " + confDir.getAbsolutePath());
        postStartupInfo.add("");

        return 0;
    }

    @Override
    public void stop() {
        if (lilyServerTestUtility != null) {
            lilyServerTestUtility.stop();
            lilyServerTestUtility = null;
        }
    }
}
