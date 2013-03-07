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
package org.lilyproject.repository.cli;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.repository.api.RepositoryTableManager;
import org.lilyproject.repository.api.RepositoryTableManager.TableCreateDescriptor;
import org.lilyproject.repository.impl.TableCreateDescriptorImpl;

/**
 * Command-line utility for creating new record tables in Lily.
 */
public class CreateTableCli extends BaseTableCliTool {

    private Option regionCountOpt;
    private Option splitKeyPrefixOpt;
    private Option splitKeysOpt;

    private TableCreateDescriptor createDescriptor;

    @SuppressWarnings("static-access")
    public CreateTableCli() {
        regionCountOpt = OptionBuilder.withArgName("regions").hasArg().withDescription(
                "Number of initial regions to create").withLongOpt("regions").create("c");

        splitKeyPrefixOpt = OptionBuilder.withArgName("prefix").hasArg().withDescription(
                "Key prefix to append to each region split").withLongOpt("prefix").create("p");

        splitKeysOpt = OptionBuilder.withArgName("keys").hasArg().withDescription(
                "Comma-separated list of split keys to be used for pre-creating region splits").withLongOpt("keys").create(
                "k");

    }

    @Override
    protected String getCmdName() {
        return "lily-create-table";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        options.add(regionCountOpt);
        options.add(splitKeyPrefixOpt);
        options.add(splitKeysOpt);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0) {
            return result;
        }

        if (cmd.getArgList().size() < 1) {
            System.err.println("Table name is mandatory");
            return 1;
        }
        String tableName = (String)cmd.getArgList().get(0);

        int regionCount = -1;
        if (cmd.hasOption(regionCountOpt.getOpt())) {
            try {
                regionCount = Integer.parseInt(cmd.getOptionValue(regionCountOpt.getOpt()));
            } catch (NumberFormatException e) {
                System.err.println("Region count must be an integer greater than 0");
                return 1;
            }
        }

        String splitKeyPrefix = null;
        if (cmd.hasOption(splitKeyPrefixOpt.getOpt())) {
            splitKeyPrefix = cmd.getOptionValue(splitKeyPrefixOpt.getOpt());
        }

        String splitKeys = null;
        if (cmd.hasOption(splitKeysOpt.getOpt())) {
            splitKeys = cmd.getOptionValue(splitKeysOpt.getOpt());
        }

        if (regionCount != -1 && splitKeys != null) {
            System.err.println("Region count and split keys cannot be combined, region count will be ignored");
        }

        if (splitKeys != null) {
            createDescriptor = TableCreateDescriptorImpl.createInstanceWithSplitKeys(tableName, splitKeyPrefix, splitKeys);
        } else if (regionCount != -1) {
            createDescriptor = TableCreateDescriptorImpl.createInstance(tableName, splitKeyPrefix, regionCount);
        } else {
            createDescriptor = TableCreateDescriptorImpl.createInstance(tableName);
        }
        return 0;
    }

    @Override
    protected int execute(RepositoryTableManager tableManager) throws InterruptedException, IOException {
        byte[][] splitKeys = createDescriptor.getSplitKeys();
        int numRegions = splitKeys != null ? splitKeys.length + 1 : 1;
        System.out.printf("Creating table '%s' with %d region%s...\n", createDescriptor.getName(), numRegions,
                numRegions == 1 ? "" : "s");
        tableManager.createTable(createDescriptor);
        System.out.printf("Table '%s' created\n", createDescriptor.getName());
        return 0;
    }

    public static void main(String[] args) {
        new CreateTableCli().start(args);
    }

}
