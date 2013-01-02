/*
 * Copyright 2010 Outerthought bvba
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

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

/**
 * Base for CLI tools that need a ZooKeeper connect string.
 */
public abstract class BaseZkCliTool extends BaseCliTool {
    private static final String DEFAULT_ZK_CONNECT = "localhost";

    protected String zkConnectionString;

    protected int zkSessionTimeout = 40000;

    protected Option zkOption;

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        zkOption = OptionBuilder
                .withArgName("connection-string")
                .hasArg()
                .withDescription("ZooKeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");

        options.add(zkOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0) {
            return result;
        }

        if (!cmd.hasOption(zkOption.getOpt())) {
            // to stderr: makes that sample config dumps of e.g. tester tool do not start with this line, and
            // can thus be redirected to a file without further editing.
            System.err.println("ZooKeeper connection string not specified, using default: " + DEFAULT_ZK_CONNECT);
            System.err.println();
            zkConnectionString = DEFAULT_ZK_CONNECT;
        } else {
            zkConnectionString = cmd.getOptionValue(zkOption.getOpt());
        }

        return 0;
    }
}
