package org.kauriproject.runtime.rapi;

import org.kauriproject.conf.Conf;

import java.util.Collection;

/**
 * Registry of {@link Conf Conf(iguration)} objects for some module.
 *
 * <p>The configuration registry consists of a tree of nodes, each node can contain
 * a {@link Conf} object and/or a list of child nodes. Each node has a name, the nodes
 * are identified (addressed) by a slash-separated path.
 */
public interface ConfRegistry {
    /**
     * Same as {@link #getConfiguration(String, boolean) getConfiguration(path, true)}.
     */
    Conf getConfiguration(String path);

    /**
     * Same as {@link #getConfiguration(String, boolean, boolean) getConfiguration(path, true, true}.
     */
    Conf getConfiguration(String path, boolean create);

    /**
     * Retrieve a configuration.
     *
     * @param path Slash separated path identifying the configuration.
     *             The path should not start with a slash, no c14n
     *             is performed on the path.
     *
     * @param create if true, an empty Conf object will be returned if
     *               the configuration does not exist.
     *               If false, and silent is true, null is returned,
     *               if silent is false, a {@link ConfNotFoundException} is thrown.
     *
     * @param silent if true, and create is false, and the conf does
     *               not exist, null will be returned instead of throwing
     *               an exception.
     */
    Conf getConfiguration(String path, boolean create, boolean silent);

    /**
     * Returns the names (not paths) of the child configurations
     * below a certain path. Does only list concrete configurations,
     * not child-paths.
     *
     * <p>If the path does not exist, an empty collection is returned.
     */
    Collection<String> getConfigurations(String path);

    /**
     * Adds a listener.
     *
     * @param path path to listen for, specify null to get notified of all changes
     *             within this ConfRegistry. The path is slash-separated, and should
     *             not start or end with a slash.
     * @param types the kinds of changes that you want to listen to. See {@link ConfListener}
     *              for a detailed description.
     */
    void addListener(ConfListener listener, String path, ConfListener.ChangeType... types);

    /**
     * Removes this listener. If this listener would have been added serveral times (for
     * different paths and change types), then all instances will be removed.
     */
    void removeListener(ConfListener listener);
}
