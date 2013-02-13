package org.lilyproject.servlet;

import javax.annotation.PostConstruct;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.logging.Logger;

import org.lilyproject.runtime.rapi.KauriModule;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.context.support.GenericWebApplicationContext;

/**
 * This class is part of the mvc project because it is mvc specific. It creates a spring mvc dispatcher servlet
 * which has the kauri spring app context as parent app context and registers this servlet with the container.
 */
public class JerseySpringServletManager {

    private static final Logger LOGGER = Logger.getLogger(JerseySpringServletManager.class.getName());

    @Autowired
    private KauriModule module;

    @Autowired
    private ServletRegistry servletRegistry;

    private List<String> urlPatterns = Collections.EMPTY_LIST;

    private final String springMvcApplicationContextLocation;

    public JerseySpringServletManager(String springMvcApplicationContextLocation) {
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

                JerseySpringServlet springServlet = new JerseySpringServlet(mvcContext);
                return springServlet;

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
