package org.kauriproject.template.security;

import java.util.List;

public interface AccessDecider {
    /**
     * Decides whether access is allowed to template output protected
     * with the given attributes.
     */
    boolean decide(List<String> attributes);
}
