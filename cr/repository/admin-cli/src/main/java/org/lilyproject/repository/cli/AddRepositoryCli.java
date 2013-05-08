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
import org.apache.commons.cli.Option;

import java.util.List;

public class AddRepositoryCli extends BaseRepositoriesAdminCli {

    @Override
    protected String getCmdName() {
        return "lily-add-repository";
    }

    public static void main(String[] args) {
        new AddRepositoryCli().start(args);
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

        String repositoryName = cmd.getOptionValue(nameOption.getOpt());
        if (repositoryName == null) {
            System.out.println("Specify a name for the repository with -" + nameOption.getOpt());
            return 1;
        }

        repositoryModel.create(repositoryName);

        System.out.println("Repository " + repositoryName + " created.");
        System.out.println();
        System.out.println("Waiting for repository to become active. This will only work when a Lily server is running.");
        System.out.println("You can safely interrupt this with ctrl+c");
        while (!repositoryModel.repositoryExistsAndActive(repositoryName)) {
            System.out.print(".");
            Thread.sleep(1000L);
        }

        return 0;
    }
}
