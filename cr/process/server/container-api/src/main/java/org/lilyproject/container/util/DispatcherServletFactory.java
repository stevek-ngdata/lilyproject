package org.lilyproject.container.util;

import javax.servlet.http.HttpServlet;
import java.util.logging.Logger;

import org.kauriproject.conf.Conf;
import org.kauriproject.runtime.rapi.KauriModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class DispatcherServletFactory {

    private static final Logger LOGGER = Logger.getLogger(DispatcherServletFactory.class.getName());

    private Conf configuration;

    @Autowired
    private KauriModule module;

    public HttpServlet createDispatcherServlet() {

        ApplicationContext springContext = module.getSpringContext();

        DispatcherServlet dispatcherServlet = new DispatcherServlet();

        // TODO: create a spring context hierarchy (services -> web stuff)
        // and set up the dispatcherservlet accordingly

        return dispatcherServlet;

    }

}