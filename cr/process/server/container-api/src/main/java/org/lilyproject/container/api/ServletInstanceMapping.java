package org.lilyproject.container.api;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServlet;

import org.springframework.beans.factory.annotation.Autowired;

public class ServletInstanceMapping {
    private String urlPattern;

    private HttpServlet servlet;

    @Autowired
    private Container container;

    public void setUrlPattern(String urlPattern) {
        this.urlPattern = urlPattern;
    }

    public void setServlet(HttpServlet servlet) {
        this.servlet = servlet;
    }

    @PostConstruct
    public void registerMapping() {
        container.addServletMapping(urlPattern, servlet);
    }
}