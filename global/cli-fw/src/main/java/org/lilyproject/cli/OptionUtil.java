package org.lilyproject.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class OptionUtil {
    public static int getIntOption(CommandLine cmd, Option option, int defaultValue) {
        if (cmd.hasOption(option.getOpt())) {
            try {
                return Integer.parseInt(cmd.getOptionValue(option.getOpt()));
            } catch (NumberFormatException e) {
                System.out.println("Invalid value for option " + option.getLongOpt() + " : " +
                        cmd.getOptionValue(option.getOpt()));
                System.exit(1);
            }
        }
        return defaultValue;
    }

    public static String getStringOption(CommandLine cmd, Option option, String defaultValue) {
        if (cmd.hasOption(option.getOpt())) {
            return cmd.getOptionValue(option.getOpt());
        } else {
            return defaultValue;
        }
    }
}
