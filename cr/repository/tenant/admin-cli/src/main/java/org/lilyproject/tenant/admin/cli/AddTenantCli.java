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

import java.util.List;

public class AddTenantCli extends BaseTenantAdminCli {

    @Override
    protected String getCmdName() {
        return "lily-add-tenant";
    }

    public static void main(String[] args) {
        new AddTenantCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        options.add(nameOption);
        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        String tenantName = cmd.getOptionValue(nameOption.getOpt());
        if (tenantName == null) {
            System.out.println("Specify a name for the tenant with -" + nameOption.getOpt());
            return 1;
        }

        tenantModel.create(tenantName);

        System.out.println("Tenant " + tenantName + " created.");
        System.out.println();
        System.out.println("Waiting for tenant to become active. This will only work when a Lily server is running.");
        System.out.println("You can safely interrupt this with ctrl+c");
        while (!tenantModel.tenantExistsAndActive(tenantName)) {
            System.out.print(".");
            Thread.sleep(1000L);
        }

        return 0;
    }
}
