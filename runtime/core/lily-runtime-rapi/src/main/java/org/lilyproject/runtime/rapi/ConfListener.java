package org.lilyproject.runtime.rapi;

/**
 * Listener callback for configuration changes.
 *
 * <p>This can be implemented by components that support adjusting their
 * configuration dynamically.
 *
 * <p>See {@link ConfRegistry#addListener}.
 *
 * <p>Just as a reminder, and as explained at {@link ConfRegistry},
 * each path-addressed node in the configuration tree can both contain
 * a {@link org.lilyproject.conf.Conf Conf} and can have children.
 *
 * <p>About the change types:
 *
 * <ul>
 * <li><p>CONF_CHANGE: listen for changes to Conf's, includes new, updated
 * and deleted confs. So if you get a change event and then try to retrieve
 * the changed config, it might fail because it has been deleted.
 *
 * <li><p>PATH_CHANGE: listen for additions and removals of new Conf's below
 * a path. Addition and removal of child-paths which do not have Conf but
 * are only a path-segment for lower-level Conf's will not get reported.
 * Maybe it is easier to comprehend this using filesystem terminology:
 * changes for a directory are only reported when files in that directory
 * are added or deleted, not when subdirectories are added or deleted.
 * Updates to files in a directory are not reported as changes either,
 * only file additions and removals are counted as changes. Note that
 * compared to a filesystem, each node in the configuration tree can
 * both be 'file' and 'directory', i.e. can contain a Conf and have
 * children.
 * </ul>
 */
public interface ConfListener {
    enum ChangeType { CONF_CHANGE, PATH_CHANGE }

    /**
     * Notifies a configuration change.
     * 
     * <p>Implementations should return reasonably quickly from this, as to
     * not block other ConfListener's of receiving notifications.
     *
     * @param path the path which was modified. If you registered this listener
     *             for one specific path, then this path will always be the
     *             same as the one that you registered the listener for.
     */
    void confAltered(String path, ChangeType changeType);
}
