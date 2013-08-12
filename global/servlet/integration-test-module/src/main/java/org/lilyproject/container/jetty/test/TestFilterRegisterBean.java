package org.lilyproject.container.jetty.test;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.ServletContext;
import java.util.Map;

import org.lilyproject.servletregistry.api.ServletFilterRegistryEntry;
import org.lilyproject.servletregistry.api.ServletRegistry;

public class TestFilterRegisterBean {

    public static final int TEST_FILTER_PRIO = 1000;

    private ServletRegistry servletRegistry;

    private Map<String, Integer> urlPatterns;

    private TestFilter testFilter;

    public TestFilterRegisterBean(ServletRegistry servletRegistry,
                                  Map<String, Integer> urlPatterns,
                                  TestFilter filter) {
        this.servletRegistry = servletRegistry;
        this.urlPatterns = urlPatterns;
        this.testFilter = filter;
    }

    @PostConstruct
    public void registerFilter(){
        servletRegistry.addFilterEntry(new ServletFilterRegistryEntry() {
            @Override
            public Map<String, Integer> getUrlPatterns() {
                return urlPatterns;
            }

            @Override
            public Filter getServletFilterInstance(ServletContext context) {
                return testFilter;
            }

            @Override
            public int getPriority() {
                return TEST_FILTER_PRIO;
            }
        });
    }
}
