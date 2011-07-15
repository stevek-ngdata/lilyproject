package org.lilyproject.lilyservertestfw.launcher;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.kauriproject.runtime.repository.ArtifactRepository;
import org.kauriproject.runtime.repository.ChainedMaven2StyleArtifactRepository;
import org.lilyproject.lilyservertestfw.ConfUtil;
import org.lilyproject.lilyservertestfw.LilyServerTestUtility;
import org.lilyproject.util.MavenUtil;

public class LilyLauncherService implements LauncherService {
    private LilyServerTestUtility lilyServerTestUtility;
    private File defaultConfDir = null;
    private ArtifactRepository artifactRepository;

    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome) throws Exception {
        String defaultConfDirPath = System.getProperty("lily.conf.dir");
        if (defaultConfDirPath != null) {
            defaultConfDir = new File(defaultConfDirPath);
        } else {
            // This is just to be sure. The LilyLauncher script should actually always set the system property
            defaultConfDir = new File(testHome, "lilyconf");
            FileUtils.forceMkdir(defaultConfDir);
            URL confUrl = getClass().getClassLoader().getResource(ConfUtil.CONF_RESOURCE_PATH);
            ConfUtil.copyConfResources(confUrl, ConfUtil.CONF_RESOURCE_PATH, defaultConfDir);
        }

        String repository = System.getProperty("lily.testlauncher.repository");
        if (repository != null) {
            // Since this is for development, we always add the local Maven repository
            repository = MavenUtil.findLocalMavenRepository() + "," + repository;
            artifactRepository = new ChainedMaven2StyleArtifactRepository(repository);
        }

        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        lilyServerTestUtility = new LilyServerTestUtility(defaultConfDir.getAbsolutePath(), null);
        lilyServerTestUtility.setArtifactRepository(artifactRepository);
        lilyServerTestUtility.start();

        postStartupInfo.add("-----------------------------------------------");
        postStartupInfo.add("Lily is running");
        postStartupInfo.add("");
        postStartupInfo.add("Using configuration from: " + defaultConfDir.getAbsolutePath());
        postStartupInfo.add("You can connect a LilyClient to it using zookeeper connect string \"localhost:2181\"");
        postStartupInfo.add("(Example: new LilyClient(\"localhost:2181\", 20000); )");
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
