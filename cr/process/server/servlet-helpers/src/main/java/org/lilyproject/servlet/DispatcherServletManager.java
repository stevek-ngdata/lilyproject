package org.lilyproject.servlet;

import javax.annotation.PostConstruct;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import org.lilyproject.runtime.runtime.rapi.KauriModule;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.context.support.GenericWebApplicationContext;
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
    private ServletRegistry servletRegistry;

    private List<String> urlPatterns = Collections.EMPTY_LIST;

    private final String springMvcApplicationContextLocation;

    public DispatcherServletManager(String springMvcApplicationContextLocation) {
        this.springMvcApplicationContextLocation = springMvcApplicationContextLocation;
    }


    @PostConstruct
    public void createAndRegisterDispatcherServletInContainer() {
        final ApplicationContext existingKauriSpringContext = module.getSpringContext();

        servletRegistry.addEntry(new ServletRegistryEntry() {
            @Override
            public List<String> getUrlPatterns() {
                return urlPatterns;
            }

            @Override
            public Servlet getServletInstance(ServletContext context) {
                final GenericWebApplicationContext mvcContext = new GenericWebApplicationContext(context);

                mvcContext.setClassLoader(module.getClassLoader());

                mvcContext.setServletContext(context);
                XmlBeanDefinitionReader xmlReader = new XmlBeanDefinitionReader(mvcContext);
                xmlReader.loadBeanDefinitions(new ClassPathResource(springMvcApplicationContextLocation));
                mvcContext.setParent(existingKauriSpringContext);
                mvcContext.refresh();

                DispatcherServlet dispatcherServlet = new DispatcherServlet(mvcContext);
                dispatcherServlet.setDetectAllHandlerMappings(true);
                return dispatcherServlet;
            }

            @Override
            public List<EventListener> getEventListeners() {
                return Collections.EMPTY_LIST;
            }
        });
    }

    public void setUrlPatterns(List<String> urlPatterns) {
        this.urlPatterns = urlPatterns;
    }
}
