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

import org.lilyproject.repository.api.RepositoryTableManager;

import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.util.Version;

public abstract class BaseTableCliTool extends BaseZkCliTool {

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-repository-admin-cli");
    }
    
    @Override
    public int run(CommandLine cmd) throws Exception {
        int status = super.run(cmd);
        if (status != 0) {
            return status;
        }
        
        LilyClient lilyClient = new LilyClient(zkConnectionString, 30000);
        RepositoryTableManager tableManager = lilyClient.getTableManager();
        try {
            status = execute(tableManager);
        } finally {
            lilyClient.close();
        }
        return status;
    }
    
    /**
     * Perform table management tasks.
     * @param tableManager manager for accessing repository tables
     * @return the exit status of the command
     */
    protected abstract int execute(RepositoryTableManager tableManager) throws Exception;
    
}
