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

public class DeleteTenantCli extends BaseTenantAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-delete-tenant";
    }

    public static void main(String[] args) {
        new DeleteTenantCli().start(args);
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
            System.out.println("Specify the name of the tenant to delete with -" + nameOption.getOpt());
            return 1;
        }

        tenantModel.delete(tenantName);

        System.out.println(String.format("Deletion of tenant '%s' has been requested.", tenantName));

        return 0;
    }
}
