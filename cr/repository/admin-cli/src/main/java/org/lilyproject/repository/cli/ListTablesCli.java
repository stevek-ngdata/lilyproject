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

import org.lilyproject.repository.api.RepositoryTableManager;

/**
 * Command-line utility for listing all record tables (i.e. tables holding {@code Record}s) in Lily.
 */
public class ListTablesCli extends BaseTableCliTool {

    @Override
    protected String getCmdName() {
        return "lily-list-tables";
    }


    @Override
    protected int execute(RepositoryTableManager tableManager) throws Exception {
        List<String> tableNames = tableManager.getTableNames();
        if (tableNames.isEmpty()) {
            System.out.println("No repository tables found");
        } else {
            System.out.printf("Number of indexes: %d\n", tableNames.size());
            for (String tableName : tableNames) {
                System.out.println("    " + tableName);
            }
        }
        return 0;
    }
    
    public static void main(String[] args) {
        new ListTablesCli().start(args);
    }

}
