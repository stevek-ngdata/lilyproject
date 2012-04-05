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
package org.lilyproject.testclientfw;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class Util {
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

    public static File getOutputFileRollOldOne(String name) throws IOException {
        File file = new File(name);
        if (file.exists()) {
            for (int i = 1; ; i++) {
                File rollFile = new File(name + "-" + i);
                if (!rollFile.exists()) {
                    FileUtils.copyFile(file, rollFile);
                    break;
                }
            }
        }

        return file;
    }

    public static PrintStream getOutputPrintStreamRollOldOne(String name) throws IOException {
        File file = getOutputFileRollOldOne(name);
        PrintStream ps = new PrintStream(new FileOutputStream(file));
        return ps;
    }

    public static <T> T pickFromList(List<T> items) {
        int selected = (int)Math.floor(Math.random() * items.size());
        return items.get(selected);
    }
}
