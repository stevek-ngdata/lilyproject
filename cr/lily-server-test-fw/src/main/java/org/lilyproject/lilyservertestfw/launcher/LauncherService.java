package org.lilyproject.lilyservertestfw.launcher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.io.File;
import java.util.List;

public interface LauncherService {

    void addOptions(List<Option> options);
    
    /**
     * Here service should read out & validate arguments. The setup method will be called for all services before
     * actually starting them, so that we can be reasonably sure starting will be successful.
     *
     * @return process exit code, if non-null process will exit. This should be used for expected error conditions,
     *         rather than throwing an exception.
     */
    int setup(CommandLine cmd, File testHome) throws Exception;

    /**
     *
     * @param postStartupInfo any messages added to this list will be printed out once startup has been completed
     *
     * @return process exit code, if non-null process will exit.
     */
    int start(List<String> postStartupInfo) throws Exception;

    /**
     * Implementations should be written such that they catch & log exceptions internally.
     */
    void stop();
}
