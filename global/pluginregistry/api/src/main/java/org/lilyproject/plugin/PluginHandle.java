package org.lilyproject.plugin;

/**
 * An object encapsulating a plugin and its name.
 */
public interface PluginHandle<T> {
    T getPlugin();
    
    String getName();
}
