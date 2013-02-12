package org.kauriproject.conf;

import org.kauriproject.util.location.Location;

import java.util.List;
import java.util.Map;

/**
 * A Configuration node.
 *
 * <p>The configuration data model consists of a tree of Conf objects. Each
 * Conf node can either have children or a value, not both. Each node can
 * have attributes.
 *
 * <p>For convenience, the node values and attribute values can be retrieved
 * as a variety of primitive types, using the get{Value|Attribute}asXYZ()
 * methods. If the conversion from string to the desired type is not possible,
 * a ConfException is thrown.
 *
 * <p>The configuration model is inspired by XML, but without mixed content.
 *
 * <p>Historical note: this configuration data model was inspired by late
 * Apache Avalon's Configuration interface.
 */
public interface Conf {
    /**
     * The list of child Conf's.
     *
     * <p>For nodes without children, this returns an empty list rather
     * than null.
     */
    List<Conf> getChildren();

    boolean hasChildren();

    List<Conf> getChildren(String name);

    /**
     * Same as {@link #getChild(String, boolean) getChild(name, true)}.
     */
    Conf getChild(String name);

    /**
     * Returns the first child with the specified name.
     *
     * <p>If there is no child with this name, and the create parameter
     * is false, null is returned. If the create parameter is true,
     * and empty configuration will be returned.
     */
    Conf getChild(String name, boolean create);

    /**
     * Returns the first child with the specified name.
     *
     * <p>Throws an exception in case there is no child with this name.
     */
    Conf getRequiredChild(String name);

    String getName();

    /**
     * Location where this configuration was loaded from.
     */
    Location getLocation();

    /**
     * Returns the value of this node, throws a ConfException if
     * the node does not have a value.
     */
    String getValue();

    boolean getValueAsBoolean();

    int getValueAsInteger();

    long getValueAsLong();

    float getValueAsFloat();

    double getValueAsDouble();

    String getValue(String defaultValue);

    Boolean getValueAsBoolean(Boolean defaultValue);

    Integer getValueAsInteger(Integer defaultValue);

    Long getValueAsLong(Long defaultValue);

    Float getValueAsFloat(Float defaultValue);

    Double getValueAsDouble(Double defaultValue);

    Map<String, String> getAttributes();

    String getAttribute(String name);

    boolean getAttributeAsBoolean(String name);

    int getAttributeAsInteger(String name);

    long getAttributeAsLong(String name);

    float getAttributeAsFloat(String name);

    double getAttributeAsDouble(String name);

    String getAttribute(String name, String defaultValue);

    Boolean getAttributeAsBoolean(String name, Boolean defaultValue);

    Integer getAttributeAsInteger(String name, Integer defaultValue);

    Long getAttributeAsLong(String name, Long defaultValue);

    Float getAttributeAsFloat(String name, Float defaultValue);

    Double getAttributeAsDouble(String name, Double defaultValue);
}
