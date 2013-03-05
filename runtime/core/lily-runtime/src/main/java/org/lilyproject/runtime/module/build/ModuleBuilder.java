/*
 * Copyright 2013 NGDATA nv
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.module.build;

import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.runtime.LilyRuntime;
import org.lilyproject.runtime.module.Module;
import org.lilyproject.runtime.module.ModuleConfig;
import org.lilyproject.runtime.module.ModuleImpl;
import org.lilyproject.runtime.module.javaservice.JavaServiceShield;
import org.lilyproject.runtime.rapi.ModuleSource;
import org.lilyproject.runtime.repository.ArtifactNotFoundException;
import org.lilyproject.util.io.IOUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.InputStreamResource;

public class ModuleBuilder {
    // These ThreadLocal's serve as communication mechanism for LilyRuntimeNamespaceHandler
    protected static ThreadLocal<SpringBuildContext> SPRING_BUILD_CONTEXT = new ThreadLocal<SpringBuildContext>();

    protected final Log infolog = LogFactory.getLog(LilyRuntime.INFO_LOG_CATEGORY);
    private final Log log = LogFactory.getLog(getClass());

    private ModuleBuilder() {
        // private constructor to avoid instantiation
    }

    public static Module build(ModuleConfig cfg, ClassLoader classLoader, LilyRuntime runtime) throws ArtifactNotFoundException, MalformedURLException {
        return new ModuleBuilder().buildInt(cfg, classLoader, runtime);
    }

    private Module buildInt(ModuleConfig cfg, ClassLoader classLoader, LilyRuntime runtime) throws ArtifactNotFoundException, MalformedURLException {
        infolog.info("Starting module " + cfg.getId() + " - " + cfg.getLocation());
        ClassLoader previousContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);

            GenericApplicationContext applicationContext = new GenericApplicationContext();
            applicationContext.setDisplayName(cfg.getId());
            applicationContext.setClassLoader(classLoader);

            // Note: before loading any beans in the spring container:
            //   * the spring build context needs access to the module, for possible injection & module-protocol resolving during bean initialization
            //   * the module also needs to have the reference to the applicationcontext, as there might be beans trying to get while initializing
            ModuleImpl module = new ModuleImpl(classLoader, applicationContext, cfg.getDefinition(), cfg.getModuleSource());

            SpringBuildContext springBuildContext = new SpringBuildContext(runtime, module, classLoader);
            SPRING_BUILD_CONTEXT.set(springBuildContext);

            XmlBeanDefinitionReader xmlReader = new XmlBeanDefinitionReader(applicationContext);
            xmlReader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD);
            xmlReader.setBeanClassLoader(classLoader);

            for (ModuleSource.SpringConfigEntry entry : cfg.getModuleSource().getSpringConfigs(runtime.getMode())) {
                InputStream is = entry.getStream();
                try {
                    xmlReader.loadBeanDefinitions(new InputStreamResource(is, entry.getLocation() + " in " + cfg.getDefinition().getFile().getAbsolutePath()));
                } finally {
                    IOUtils.closeQuietly(is, entry.getLocation());
                }
            }
            applicationContext.refresh();

            // Handle the service exports
            for (SpringBuildContext.JavaServiceExport entry : springBuildContext.getExportedJavaServices()) {
                Class serviceType = entry.serviceType;
                if (!serviceType.isInterface())
                    throw new LilyRTException("Exported service is not an interface: " + serviceType.getName());

                String beanName = entry.beanName;
                Object component;
                try {
                    component = applicationContext.getBean(beanName);
                } catch (NoSuchBeanDefinitionException e) {
                    throw new LilyRTException("Bean not found for service to export, service type " + serviceType.getName() + ", bean name " + beanName, e);
                }

                if (!serviceType.isAssignableFrom(component.getClass()))
                    throw new LilyRTException("Exported service does not implemented specified type interface. Bean = " + beanName + ", interface = " + serviceType.getName());

                infolog.debug(" exporting bean " + beanName + " for service " + serviceType.getName());
                Object service = shieldJavaService(serviceType, component, module, classLoader);
                runtime.getJavaServiceManager().addService(serviceType, cfg.getId(), entry.name, service);
            }

            module.start();
            return module;
        } catch (Throwable e) {
            // TODO module source and classloader handle might need disposing!
            // especially important if the lily runtime is launched as part of a longer-living VM
            throw new LilyRTException("Error constructing module defined at " + cfg.getDefinition().getFile().getAbsolutePath(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previousContextClassLoader);
            SPRING_BUILD_CONTEXT.set(null);
        }
    }

    /**
     * Wraps the bean to assure only that only methods of the published service
     * can be accessed.
     */
    private Object shieldJavaService(Class serviceInterface, Object bean, Module module, ClassLoader classLoader) {
        JavaServiceShield handler = new JavaServiceShield(bean, module, serviceInterface, classLoader);
        return Proxy.newProxyInstance(classLoader, new Class[] { serviceInterface }, handler);
    }
}
