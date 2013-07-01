package org.lilyproject.servletregistry.api;

import javax.servlet.Filter;
import javax.servlet.ServletContext;
import java.util.Map;

public interface ServletFilterRegistryEntry {
    /**
     * map of url pattern and dispatch type. Set type to 0 if unsure. @see org.mortbay.jetty.Handler#DEFAULT
     */
    Map<String, Integer> getUrlPatterns();

    Filter getServletFilterInstance(ServletContext context);

    /**
     * Determines the order in which filters are added. Lowest value prio = added first.
     */
    int getPriority();
}
