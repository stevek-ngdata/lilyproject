package org.kauriproject.template;

import org.xml.sax.helpers.NamespaceSupport;

/**
 * Allows lazy access to the namespace-state.
 */
public interface NamespacesHandle {
    /**
     * Returns a clone of the current state of the NamespaceSupport.
     */
    NamespaceSupport snapshot();

    /**
     * Returns a reference to the live NamespaceSupport used during
     * template parsing. Use this if don't need to hold on to the
     * NamespaceSupport instance.
     */
    NamespaceSupport get();
}
