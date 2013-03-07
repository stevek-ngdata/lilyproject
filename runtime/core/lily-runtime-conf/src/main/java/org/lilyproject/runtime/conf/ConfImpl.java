/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.conf;

import org.lilyproject.util.location.Location;
import org.lilyproject.util.location.LocationImpl;
import org.lilyproject.runtime.conf.jxpath.ConfPointerFactory;
import org.apache.commons.jxpath.JXPathContext;

import java.util.*;

public class ConfImpl implements Conf {
    private List<ConfImpl> children = new ArrayList<ConfImpl>();
    private Map<String, String> attributes = new HashMap<String, String>();
    private String name;
    private String value;
    private Location location;
    private Inheritance inheritance;
    /**
     * Inheritance uniqueness constraint: an expression (JXPath) used when inheriting to determine
     * if a node is already present.
     * */
    private String inheritConstraint;

    public enum Inheritance { NONE, SHALLOW, DEEP }

    static {
        ConfPointerFactory.register();
    }

    public ConfImpl(String name, Location location) {
        if (name == null) {
            throw new IllegalArgumentException("Null argument: name");
        }
        if (location == null) {
            throw new IllegalArgumentException("Null argument: location");
        }

        this.name = name;
        this.location = location;
    }

    public void addChild(ConfImpl config) {
        if (config == null) {
            throw new IllegalArgumentException("Null argument: config");
        }
        this.children.add(config);
    }

    public void setValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Null argument: config");
        }
        if (value.length() != value.trim().length()) {
            throw new IllegalArgumentException("Configuration values should be trimmed.");
        }
        if (value.length() == 0) {
            throw new IllegalArgumentException("Configuration values should have a length > 0.");
        }

        this.value = value;
    }

    public void addAttribute(String name, String value) {
        attributes.put(name, value);
    }

    public List<Conf> getChildren() {
        return Collections.<Conf>unmodifiableList(children);
    }

    public boolean hasChildren() {
        return children.size() > 0;
    }

    public List<Conf> getChildren(String name) {
        List<Conf> result = new ArrayList<Conf>();

        for (Conf conf : children) {
            if (conf.getName().equals(name)) {
                result.add(conf);
            }
        }

        return Collections.unmodifiableList(result);
    }

    public Conf getChild(String name) {
        return getChild(name, true);
    }

    public Conf getChild(String name, boolean create) {
        for (Conf conf : children) {
            if (conf.getName().equals(name)) {
                return conf;
            }
        }

        if (create) {
            ConfImpl config =  new ConfImpl(name, new LocationImpl(null,
                    "<generated>" + location.getURI(), location.getLineNumber(), location.getColumnNumber()));
            return config;
        } else {
            return null;
        }
    }

    public Conf getRequiredChild(String name) {
        Conf child = getChild(name, false);
        if (child != null) {
            return child;
        } else {
            throw new ConfException("Missing configuration node \"" + name + "\" inside " + name + " at " + getLocation());
        }
    }

    public String getName() {
        return name;
    }

    public Location getLocation() {
        return location;
    }

    public String getValue() {
        checkValuePresent();
        return value;
    }

    public boolean getValueAsBoolean() {
        return convertToBoolean(getValue(), null);
    }

    public int getValueAsInteger() {
        return convertToInteger(getValue(), null);
    }

    public long getValueAsLong() {
        return convertToLong(getValue(), null);
    }

    public float getValueAsFloat() {
        return convertToFloat(getValue(), null);
    }

    public double getValueAsDouble() {
        return convertToDouble(getValue(), null);
    }

    public String getValue(String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValue();
    }

    public Boolean getValueAsBoolean(Boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValueAsBoolean();
    }

    public Integer getValueAsInteger(Integer defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValueAsInteger();
    }

    public Long getValueAsLong(Long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValueAsLong();
    }

    public Float getValueAsFloat(Float defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValueAsFloat();
    }

    public Double getValueAsDouble(Double defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return getValueAsDouble();
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    public String getAttribute(String name) {
        checkAttributePresent(name);
        return attributes.get(name);
    }

    public boolean getAttributeAsBoolean(String name) {
        return convertToBoolean(getAttribute(name), name);
    }

    public int getAttributeAsInteger(String name) {
        return convertToInteger(getAttribute(name), name);
    }

    public long getAttributeAsLong(String name) {
        return convertToLong(getAttribute(name), name);
    }

    public float getAttributeAsFloat(String name) {
        return convertToFloat(getAttribute(name), name);
    }

    public double getAttributeAsDouble(String name) {
        return convertToDouble(getAttribute(name), name);
    }

    public String getAttribute(String name, String defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttribute(name);
    }

    public Boolean getAttributeAsBoolean(String name, Boolean defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttributeAsBoolean(name);
    }

    public Integer getAttributeAsInteger(String name, Integer defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttributeAsInteger(name);
    }

    public Long getAttributeAsLong(String name, Long defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttributeAsLong(name);
    }

    public Float getAttributeAsFloat(String name, Float defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttributeAsFloat(name);
    }

    public Double getAttributeAsDouble(String name, Double defaultValue) {
        if (!attributes.containsKey(name)) {
            return defaultValue;
        }
        return getAttributeAsDouble(name);
    }

    private void checkValuePresent() {
        if (value == null) {
            throw new ConfException("No value is associated with the configuration element " + getName()
                    + " at " + getLocation());
        }
    }

    private void checkAttributePresent(String attributeName) {
        if (!attributes.containsKey(attributeName)) {
            throw new ConfException("No attribute \"" + attributeName + "\" on the configuration element "
                    + getName() + " at " + getLocation());
        }
    }

    private boolean convertToBoolean(String value, String attributeName) {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("t")
                || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("y")) {
            return true;
        } else if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("f")
                || value.equalsIgnoreCase("no") || value.equalsIgnoreCase("n")) {
            return false;
        } else {
            produceBadFormatException("boolean", value, attributeName);
            return true; // never reached
        }
    }

    private int convertToInteger(String value, String attributeName) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            produceBadFormatException("integer", value, attributeName);
            return 0; // never reached
        }
    }

    private long convertToLong(String value, String attributeName) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            produceBadFormatException("long", value, attributeName);
            return 0; // never reached
        }
    }

    private float convertToFloat(String value, String attributeName) {
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            produceBadFormatException("float", value, attributeName);
            return 0; // never reached
        }
    }

    private double convertToDouble(String value, String attributeName) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            produceBadFormatException("double", value, attributeName);
            return 0; // never reached
        }
    }

    private void produceBadFormatException(String typeName, String value, String attributeName) {
        StringBuilder message = new StringBuilder();
        message.append("Configuration value is not a valid ").append(typeName).append(": \"").append(value).append("\"");
        if (attributeName == null) {
            message.append(" in configuration element ").append(getName()).append(" at ").append(getLocation());
        } else {
            message.append(" in attribute ").append(attributeName).append(" of configuration element ")
                    .append(getName()).append(" at ").append(getLocation());
        }
        throw new ConfException(message.toString());
    }

    public Inheritance getInheritance() {
        return inheritance;
    }

    public void setInheritance(Inheritance inheritance) {
        this.inheritance = inheritance;
    }

    public String getInheritConstraint() {
        return inheritConstraint;
    }

    public void setInheritConstraint(String inheritConstraint) {
        this.inheritConstraint = inheritConstraint;
    }

    public void inherit(ConfImpl parent) {
        inherit(parent, Inheritance.NONE);
    }

    public void inherit(ConfImpl parent, Inheritance defaultInheritance) {
        // In case of deep inheritance, we let our children inherit from the corresponding
        // children of the parent.
        //
        // But what are corresponding children?
        //
        //  - in case there is an inheritance constraint defined, we take the first child
        //    from the parent which has the same value for the inheritance constraint.
        //
        //  - otherwise, corresponding children are children with the same name, thus
        //    an inheritance constraint "local-name()"
        //
        // Note that performance is not very critical of this routine, as it is supposed
        // that (inherited) configurations are cached.
        //
        // Note that we first perform inheritance for our children (= deep inheritance),
        // before ourselve, to avoid that we would do deep inheritance on our inherited
        // children too.

        Inheritance inheritance = this.inheritance != null ? this.inheritance : defaultInheritance;
        String inheritConstraint = this.inheritConstraint != null ? this.inheritConstraint : "local-name()";

        if (inheritance == Inheritance.DEEP && inheritConstraint.length() != 0) {
            for (ConfImpl child : children) {
                String key = evalInheritanceConstraint(child, inheritConstraint);

                for (ConfImpl parentChild : parent.children) {
                    String parentKey = evalInheritanceConstraint(parentChild, inheritConstraint);
                    if (key.equals(parentKey)) {
                        child.inherit(parentChild, inheritance);
                        break;
                    }
                }
            }
        }

        if (inheritance != Inheritance.NONE) {
            // Inherit attributes
            for (Map.Entry<String, String> attr : parent.getAttributes().entrySet()) {
                if (!attributes.containsKey(attr.getKey())) {
                    addAttribute(attr.getKey(), attr.getValue());
                }
            }

            // Calculate the set of keys for the current children
            Set<String> presentKeys = null;
            if (inheritConstraint.length() != 0) {
                presentKeys = new HashSet<String>();
                for (Conf conf : children) {
                    String value = evalInheritanceConstraint(conf, inheritConstraint);
                    if (value != null) {
                        presentKeys.add(value);
                    }
                }
            }

            // Inherit child elements
            for (ConfImpl conf : parent.children) {
                if (inheritConstraint.length() == 0) {
                    // empty inherit constraint means: inherit everything
                    children.add(conf.deepClone());
                } else {
                    String key = evalInheritanceConstraint(conf, inheritConstraint);
                    if (key != null && !presentKeys.contains(key)) {
                        children.add(conf.deepClone());
                        presentKeys.add(key);
                    }
                }
            }
        }

    }

    public String evalInheritanceConstraint(Conf conf, String inheritConstraint) {
        try {
            JXPathContext jxpc = JXPathContext.newContext(conf);
            return (String)jxpc.getValue(inheritConstraint);
        } catch (Exception e) {
            throw new ConfException("Error evaluating configuration inheritance uniqueness constraint expression: \""
                    + inheritConstraint + "\" on conf defined at " + conf.getLocation(), e);
        }
    }

    /**
     * Makes a deep clone of this Conf, the inheritance level
     * is however not cloned.
     */
    public ConfImpl deepClone() {
        ConfImpl conf = new ConfImpl(name, location);
        conf.attributes.putAll(this.attributes);
        conf.value = value;

        for (ConfImpl childConf : children) {
            conf.children.add(childConf.deepClone());
        }

        return conf;
    }
}
