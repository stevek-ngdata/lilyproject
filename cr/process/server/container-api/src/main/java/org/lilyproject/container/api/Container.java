package org.lilyproject.container.api;

import javax.servlet.http.HttpServlet;

public interface Container {

    void addServletMapping(String urlPattern, HttpServlet servlet);

}
