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
package org.lilyproject.runtime;

public enum SharingConflictResolution {
    
    HIGHEST("highest"),
    ERROR("error"),
    DONTSHARE("dontshare");
    
    private String name;
    
    private SharingConflictResolution(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public static SharingConflictResolution fromString(String name) {
        if (HIGHEST.name.equals(name))
            return HIGHEST;
        if (ERROR.name.equals(name))
            return ERROR;
        if (DONTSHARE.name.equals(name))
            return DONTSHARE;
        
        return null;
    }

}
