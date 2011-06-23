package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.io.File;
import java.util.List;

public class LilyLauncherService implements LauncherService {
    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome) throws Exception {
        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        return 0;
    }

    @Override
    public void stop() {
    }
}
