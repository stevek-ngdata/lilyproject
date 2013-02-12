/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.runtime.source;

import org.apache.commons.jci.monitor.FilesystemAlterationMonitor;

import java.io.*;

public class ExpandedJarModuleSource extends AbstractDirectoryModuleSource {
    protected ExpandedJarModuleSource(File dir, FilesystemAlterationMonitor fam) throws IOException {
        super(dir,
                new File(dir, "KAURI-INF"),
                null,
                new File(dir, "KAURI-INF/spring"),
                new File(dir, "KAURI-INF/classloader.xml"),
                dir,
                fam);
    }

    public boolean isSourceMode() {
        return false;
    }
}
