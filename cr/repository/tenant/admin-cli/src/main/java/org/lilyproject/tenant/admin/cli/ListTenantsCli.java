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
import org.lilyproject.tenant.model.api.Tenant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ListTenantsCli extends BaseTenantAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-list-tenants";
    }

    public static void main(String[] args) {
        new ListTenantsCli().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        List<Tenant> tenants = new ArrayList<Tenant>(tenantModel.getTenants());
        Collections.sort(tenants, new TenantComparator());

        System.out.println("Tenants:");
        for (Tenant tenant : tenants) {
            System.out.println(" * " + tenant.getName() + " - " + tenant.getLifecycleState().toString());
        }

        return 0;
    }

    private static class TenantComparator implements Comparator<Tenant> {
        @Override
        public int compare(Tenant o1, Tenant o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }
}
