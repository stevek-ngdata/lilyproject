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
