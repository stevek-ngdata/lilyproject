/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.tenant.admin.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.tenant.model.impl.TenantModelImpl;
import org.lilyproject.util.Version;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;

public abstract class BaseTenantAdminCli extends BaseZkCliTool {
    private ZooKeeperItf zk;
    protected TenantModel tenantModel;

    protected Option forceOption;
    protected Option nameOption;

    public BaseTenantAdminCli() {
        nameOption = OptionBuilder
                .withArgName("name")
                .hasArg()
                .withDescription("Tenant name.")
                .withLongOpt("name")
                .create("n");

        forceOption = OptionBuilder
                .withDescription("Skips optional validations.")
                .withLongOpt("force")
                .create("f");
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-tenant-admin-cli");
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);

        boolean lilyNodeExists = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                return zk.exists("/lily", false) != null;
            }
        });

        if (!lilyNodeExists) {
            if (!cmd.hasOption(forceOption.getOpt())) {
                System.out.println("No /lily node found in ZooKeeper. Are you sure you are connecting to the right");
                System.out.println("ZooKeeper? If so, use the option --" + forceOption.getLongOpt() +
                        " to bypass this check.");
                return 1;
            } else {
                System.out.println("No /lily node found in ZooKeeper. Will continue anyway since you supplied --" +
                        forceOption.getLongOpt());
                System.out.println();
            }
        }

        tenantModel = new TenantModelImpl(zk);

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(tenantModel);
        Closer.close(zk);
        super.cleanup();
    }
}
