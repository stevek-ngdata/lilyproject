package org.lilyproject.hello.webmvc;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.kauriproject.runtime.rapi.KauriModule;
import org.lilyproject.container.api.Container;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

/**
 * This class is part of the mvc project because it is mvc specific. It creates a spring mvc dispatcher servlet
 * which has the kauri spring app context as parent app context and registers this servlet with the container.
 */
public class DispatcherServletManager {

    private static final Logger LOGGER = Logger.getLogger(DispatcherServletManager.class.getName());

    @Autowired
    private KauriModule module;

    @Autowired
    private Container container;

    private final String springMvcApplicationContextLocation;

    public DispatcherServletManager(String springMvcApplicationContextLocation) {
        this.springMvcApplicationContextLocation = springMvcApplicationContextLocation;
    }


    @PostConstruct
    public void createAndRegisterDispatcherServletInContainer() {

        ApplicationContext existingKauriSpringContext = module.getSpringContext();

        XmlWebApplicationContext mvcContext = new XmlWebApplicationContext();
        mvcContext.setConfigLocation(springMvcApplicationContextLocation);
        mvcContext.setParent(existingKauriSpringContext);

        final DispatcherServlet dispatcherServlet = new DispatcherServlet(mvcContext);
        dispatcherServlet.setDetectAllHandlerMappings(true);
        container.addServletMapping("/", dispatcherServlet);
    }

}