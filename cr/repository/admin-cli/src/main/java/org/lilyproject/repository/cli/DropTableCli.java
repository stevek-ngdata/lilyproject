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

import org.apache.commons.cli.CommandLine;
import org.lilyproject.repository.api.TableManager;

public class DropTableCli extends BaseTableCliTool {

    private String tableName;

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

        if (cmd.getArgList().size() < 1) {
            System.err.println("Table name is mandatory");
            return 1;
        }
        tableName = (String)cmd.getArgList().get(0);
        return 0;
    }

    @Override
    protected int execute(TableManager tableManager) throws Exception {
        System.out.printf("Dropping table '%s'...\n", tableName);
        tableManager.dropTable(tableName);
        System.out.printf("Table '%s' in repository '%s' dropped\n", tableName, repositoryName);
        return 0;
    }

    public static void main(String[] args) {
        new DropTableCli().start(args);
    }


}
