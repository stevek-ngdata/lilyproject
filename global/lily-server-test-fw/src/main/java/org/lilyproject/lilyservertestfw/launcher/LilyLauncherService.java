package org.lilyproject.lilyservertestfw.launcher;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.lilyproject.lilyservertestfw.ConfUtil;
import org.lilyproject.lilyservertestfw.LilyServerTestUtility;

public class LilyLauncherService implements LauncherService {
    private LilyServerTestUtility lilyServerTestUtility;
    private Option confDirOption;
    private File confDir;

    @Override
    public void addOptions(List<Option> options) {
        confDirOption = OptionBuilder
            .withDescription("Lily configuration directory")
            .withLongOpt("confDir")
            .hasArg()
            .create("confDir");
        options.add(confDirOption);
    }

    @Override
    public int setup(CommandLine cmd, File testHome) throws Exception {
        String confDirName = cmd.getOptionValue(confDirOption.getOpt());
        // TODO need to make up mind about what conf dirs to use, for now just using the built-in default conf
        confDir = null;
        if (confDirName != null)
            confDir = new File(confDirName);
        else {
            confDir = new File(testHome, "lilyconf");
            FileUtils.forceMkdir(confDir);
            URL confUrl = getClass().getClassLoader().getResource(ConfUtil.CONF_RESOURCE_PATH);
            ConfUtil.copyConfResources(confUrl, ConfUtil.CONF_RESOURCE_PATH, confDir);
        }
        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
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
