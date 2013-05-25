/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class OptionUtil {

    private OptionUtil() {
    }

    public static int getIntOption(CommandLine cmd, Option option, int defaultValue) {
        String opt = option.getOpt() == null ? option.getLongOpt() : option.getOpt();
        if (cmd.hasOption(opt)) {
            try {
                return Integer.parseInt(cmd.getOptionValue(opt));
            } catch (NumberFormatException e) {
                throw new CliException("Invalid value for option " + option.getLongOpt() + ": " +
                        cmd.getOptionValue(opt));
            }
        }
        return defaultValue;
    }

    public static String getStringOption(CommandLine cmd, Option option, String defaultValue) {
        String opt = option.getOpt() == null ? option.getLongOpt() : option.getOpt();
        if (cmd.hasOption(opt)) {
            return cmd.getOptionValue(opt);
        } else {
            return defaultValue;
        }
    }

    public static <T extends Enum<T>> T getEnum(CommandLine cmd, Option option, T defaultValue, Class<T> enumClass) {
        String value = getStringOption(cmd, option, null);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(enumClass, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new CliException("Invalid value for option " + option.getLongOpt() + ": " + value);
        }
    }
}
