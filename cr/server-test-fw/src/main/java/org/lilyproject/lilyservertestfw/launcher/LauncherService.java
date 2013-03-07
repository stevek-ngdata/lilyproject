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
package org.lilyproject.lilyservertestfw.launcher;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface LauncherService {

    void addOptions(List<Option> options);

    /**
     * Here service should read out & validate arguments. The setup method will be called for all services before
     * actually starting them, so that we can be reasonably sure starting will be successful.
     *
     * @return process exit code, if non-null process will exit. This should be used for expected error conditions,
     *         rather than throwing an exception.
     */
    int setup(CommandLine cmd, File testHome, boolean clearData) throws Exception;

    /**
     *
     * @param postStartupInfo any messages added to this list will be printed out once startup has been completed
     *
     * @return process exit code, if non-null process will exit.
     */
    int start(List<String> postStartupInfo) throws Exception;

    /**
     * Implementations should be written such that they catch & log exceptions internally.
     */
    void stop();
}
