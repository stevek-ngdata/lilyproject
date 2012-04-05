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
package org.lilyproject.tools.recordrowvisualizer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SystemFields {
    Map<String, SystemField> systemFields = new HashMap<String, SystemField>();

    public static class SystemField {
        String name;
        Map<Long, Object> values = new HashMap<Long, Object>();
    }

    public SystemField getOrCreateSystemField(String name) {
        SystemField systemField = systemFields.get(name);
        if (systemField == null) {
            systemField = new SystemField();
            systemField.name = name;
            systemFields.put(name, systemField);
        }
        return systemField;
    }

    public void collectVersions(Set<Long> versions) {
        for (SystemField sysField : systemFields.values()) {
            versions.addAll(sysField.values.keySet());
        }
    }

    public Set<String> getNames() {
        TreeSet<String> names = new TreeSet<String>();
        names.addAll(systemFields.keySet());
        return names;
    }

    public Object getValue(String fieldName, Long version) {
        return systemFields.get(fieldName).values.get(version);
    }
}
