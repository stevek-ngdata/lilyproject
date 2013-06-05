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
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.repository.model.api.RepositoryExistsException;

import java.util.List;

public class AddRepositoryCli extends BaseRepositoriesAdminCli {

    protected Option failIfExistsOption;

    @Override
    protected String getCmdName() {
        return "lily-add-repository";
    }

    public static void main(String[] args) {
        new AddRepositoryCli().start(args);
    }

    @Override
    @SuppressWarnings("static-access")
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        options.add(nameOption);

        failIfExistsOption = OptionBuilder
                .withDescription("Fails if the repository already exists (process status code 1).")
                .withLongOpt("fail-if-exists")
                .create();
        options.add(failIfExistsOption);

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

        boolean existed = false;
        try {
            repositoryModel.create(repositoryName);
        } catch (RepositoryExistsException e) {
            existed = true;
        }

        if (existed) {
            System.out.println("Repository " + repositoryName + " already exists.");
            if (cmd.hasOption(failIfExistsOption.getLongOpt())) {
                return 1;
            }
        } else {
            System.out.println("Repository " + repositoryName + " created.");
        }
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
