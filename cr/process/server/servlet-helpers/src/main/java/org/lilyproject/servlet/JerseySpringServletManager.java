/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.servlet;

import javax.annotation.PostConstruct;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;

import org.lilyproject.runtime.rapi.LilyRuntimeModule;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.context.support.GenericWebApplicationContext;

public class JerseySpringServletManager {

    @Autowired
    private LilyRuntimeModule module;

    @Autowired
    private ServletRegistry servletRegistry;

    private List<String> urlPatterns = Collections.emptyList();

    private final String springMvcApplicationContextLocation;

    public JerseySpringServletManager(String springMvcApplicationContextLocation) {
        this.springMvcApplicationContextLocation = springMvcApplicationContextLocation;
    }


    @PostConstruct
    public void createAndRegisterDispatcherServletInContainer() {
        final ApplicationContext existingLilyRuntimeSpringContext = module.getSpringContext();

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
                mvcContext.setParent(existingLilyRuntimeSpringContext);
                mvcContext.refresh();

                JerseySpringServlet springServlet = new JerseySpringServlet(mvcContext);
                return springServlet;

            }

            @Override
            public List<EventListener> getEventListeners() {
                return Collections.emptyList();
            }
        });
    }

    public void setUrlPatterns(List<String> urlPatterns) {
        this.urlPatterns = urlPatterns;
    }
}
