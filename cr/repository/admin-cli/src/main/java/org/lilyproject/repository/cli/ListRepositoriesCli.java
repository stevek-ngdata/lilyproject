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
package org.lilyproject.repository.cli;

import org.apache.commons.cli.CommandLine;
import org.lilyproject.repository.model.api.RepositoryDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ListRepositoriesCli extends BaseRepositoriesAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-list-repositories";
    }

    public static void main(String[] args) {
        new ListRepositoriesCli().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        List<RepositoryDefinition> repositories = new ArrayList<RepositoryDefinition>(repositoryModel.getRepositories());
        Collections.sort(repositories, new RepositoryDefinitionComparator());

        System.out.println("Repositories:");
        for (RepositoryDefinition repository : repositories) {
            System.out.println(" * " + repository.getName() + " - " + repository.getLifecycleState().toString());
        }

        return 0;
    }

    private static class RepositoryDefinitionComparator implements Comparator<RepositoryDefinition> {
        @Override
        public int compare(RepositoryDefinition o1, RepositoryDefinition o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }
}
