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

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.repository.api.RepositoryTableManager;

public class DropTableCli extends BaseTableCliTool {
    
    private Option tableNameOpt;
    
    private String tableName;
    
    @SuppressWarnings("static-access")
    public DropTableCli() {
        tableNameOpt = OptionBuilder
                            .withArgName("table")
                            .hasArg()
                            .withDescription("Name of the table to delete")
                            .withLongOpt("table")
                            .create("t");
    }
    
    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        options.add(tableNameOpt);
        return options;
    }

    @Override
    protected String getCmdName() {
        return "lily-drop-table";
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int status = super.processOptions(cmd);
        if (status != 0) {
            return status;
        }
        if (cmd.hasOption(tableNameOpt.getOpt())) {
            tableName = cmd.getOptionValue(tableNameOpt.getOpt());
            return 0;
        } else {
            System.err.println("Table name is mandatory");
            return 1;
        }
    }
    
    @Override
    protected int execute(RepositoryTableManager tableManager) throws Exception {
        System.out.printf("Dropping table '%s'...\n", tableName);
        tableManager.dropTable(tableName);
        System.out.printf("Table '%s' dropped\n", tableName);
        return 0;
    }
    
    public static void main(String[] args) {
        new DropTableCli().start(args);
    }


}
