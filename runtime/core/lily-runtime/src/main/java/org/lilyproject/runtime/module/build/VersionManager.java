/*
 * Copyright 2013 NGDATA nv
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.module.build;

import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.conf.Conf;

public class VersionManager {
    
    private LilyRuntime runtime;
    
    public VersionManager(LilyRuntime runtime) {
        this.runtime = runtime;
    }
    
    public String getPreferredVersion(String groupId, String artifactId) {
        Conf versionsConf = runtime.getConfManager().getRuntimeConfRegistry().getConfiguration("versions", false, true);
        String preferred = null;

        if (versionsConf != null) {
            for (Conf conf: versionsConf.getChildren("version")) {
                String vGroupId = conf.getAttribute("groupId", null);
                String vArtifactId = conf.getAttribute("artifactId", null);
                
                if ((vGroupId == null || vGroupId.equals(groupId)) &&
                        (vArtifactId == null || vArtifactId.equals(artifactId))) {
                    preferred = conf.getAttribute("version", null);
                }
            }
        }
        
        /** To avoid having to create a versions.xml configuration for existing applications, always
         * return a preferred version for org.lilyproject:* items */ 
        if (preferred == null && groupId.equals("org.lilyproject")) {
            preferred = LilyRuntime.getVersion();
        }
        
        return preferred;
    }
}
