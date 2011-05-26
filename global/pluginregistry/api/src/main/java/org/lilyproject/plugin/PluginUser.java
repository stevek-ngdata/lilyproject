package org.lilyproject.plugin;

/**
 * Interface to be implemented by the user of a particular
 * type of plugin.
 *
 * <p>The {@link #pluginAdded} and {@link #pluginRemoved} methods will
 * never be called concurrently by multiple threads.
 */
public interface PluginUser<T> {
    /**
     * Notification of an available plugin.
     *
     * <p>Plugin users can be guaranteed that there won't be two
     * plugins registred with the same name (unless previously removed via
     * {@link #pluginRemoved}. The name also won't be null, an empty string,
     * or a whitespace string.
     */
    void pluginAdded(PluginHandle<T> pluginHandle);

    void pluginRemoved(PluginHandle<T> pluginHandle);
}
