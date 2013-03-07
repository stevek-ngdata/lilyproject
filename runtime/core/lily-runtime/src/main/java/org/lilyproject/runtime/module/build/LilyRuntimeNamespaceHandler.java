/*
 * Copyright 2013 NGDATA nv
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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

import org.lilyproject.runtime.LilyRTException;
import org.lilyproject.runtime.rapi.LilyRuntimeModule;
import org.lilyproject.runtime.rapi_impl.LilyRuntimeModuleImpl;
import org.springframework.beans.factory.xml.NamespaceHandler;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.lilyproject.runtime.model.JavaServiceInjectDefinition;
import org.lilyproject.runtime.module.javaservice.JavaServiceManager;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.runtime.conf.Conf;
import org.apache.commons.jxpath.JXPathContext;

import java.util.Map;
import java.util.HashMap;

public class LilyRuntimeNamespaceHandler implements NamespaceHandler {

    public void init() {
    }

    public BeanDefinition parse(Element element, ParserContext parserContext) {
        SpringBuildContext springBuildContext = ModuleBuilder.SPRING_BUILD_CONTEXT.get();

        String elementName = element.getLocalName();
        ElementProcessor processor = ELEMENT_PROCESSORS.get(elementName);

        if (processor != null) {
            try {
                return processor.process(element, parserContext, springBuildContext);
            } catch (Throwable e) {
                throw new LilyRTException("Error handling " + elementName + " directive.", e);
            }
        }

        return null;
    }

    public BeanDefinitionHolder decorate(Node node, BeanDefinitionHolder beanDefinitionHolder, ParserContext parserContext) {
        return null;
    }

    private static interface ElementProcessor {
        BeanDefinition process(Element element, ParserContext parserContext, SpringBuildContext springBuildContext) throws Exception;
    }

    private static final ElementProcessor MODULE_PROCESSOR = new ElementProcessor() {
        public BeanDefinition process(Element element, ParserContext parserContext, SpringBuildContext springBuildContext) {
            String classLoaderBeanId = element.getAttribute("classLoader");
            if (classLoaderBeanId.length() > 0) {
                RootBeanDefinition def = new RootBeanDefinition(ObjectFactoryBean.class);

                ConstructorArgumentValues args = new ConstructorArgumentValues();
                args.addIndexedArgumentValue(0, ClassLoader.class);
                args.addIndexedArgumentValue(1, springBuildContext.getModuleClassLoader());

                def.setConstructorArgumentValues(args);
                def.setLazyInit(false);

                parserContext.getRegistry().registerBeanDefinition(classLoaderBeanId, def);
            }

            String handleBeanId = element.getAttribute("handle");
            if (handleBeanId.length() > 0) {
                RootBeanDefinition def = new RootBeanDefinition(ObjectFactoryBean.class);

                ConstructorArgumentValues args = new ConstructorArgumentValues();
                args.addIndexedArgumentValue(0, LilyRuntimeModule.class);
                args.addIndexedArgumentValue(1, new LilyRuntimeModuleImpl(springBuildContext.getModule(), springBuildContext.getRuntime()));

                def.setConstructorArgumentValues(args);
                def.setLazyInit(false);

                parserContext.getRegistry().registerBeanDefinition(handleBeanId, def);
            }

            String confBeanId = element.getAttribute("conf");
            if (confBeanId.length() > 0) {
                RootBeanDefinition def = new RootBeanDefinition(ObjectFactoryBean.class);

                String moduleId = springBuildContext.getModule().getDefinition().getId();
                ConfRegistry confRegistry = springBuildContext.getRuntime().getConfManager().getConfRegistry(moduleId);

                ConstructorArgumentValues args = new ConstructorArgumentValues();
                args.addIndexedArgumentValue(0, ConfRegistry.class);
                args.addIndexedArgumentValue(1, confRegistry);

                def.setConstructorArgumentValues(args);
                def.setLazyInit(false);

                parserContext.getRegistry().registerBeanDefinition(confBeanId, def);
            }

            return null;
        }
    };

    private static final ElementProcessor IMPORT_SERVICE_PROCESSOR = new ElementProcessor() {
        public BeanDefinition process(Element element, ParserContext parserContext, SpringBuildContext springBuildContext) throws ClassNotFoundException {
            String service = element.getAttribute("service");
            Class serviceClass = parserContext.getReaderContext().getBeanClassLoader().loadClass(service);
            JavaServiceManager javaServiceManager = springBuildContext.getRuntime().getJavaServiceManager();

            String id = element.getAttribute("id");

            String dependencyName = element.getAttribute("name");
            if (dependencyName.equals("")) {
                dependencyName = id;
            }

            Object component;
            try {
                JavaServiceInjectDefinition injectDef = springBuildContext.getModule().getDefinition().getJavaServiceInject(dependencyName);
                if (injectDef == null) {
                    injectDef = springBuildContext.getModule().getDefinition().getJavaServiceInjectByService(serviceClass.getName());
                }

                if (injectDef != null) {
                    String moduleId = injectDef.getSourceModuleId();
                    String name = injectDef.getSourceJavaServiceName();
                    if (moduleId != null && name != null) {
                        component = javaServiceManager.getService(serviceClass, moduleId, name);
                    } else {
                        component = javaServiceManager.getService(serviceClass, moduleId);
                    }
                } else {
                    component = javaServiceManager.getService(serviceClass);
                }
            } catch (Throwable t) {
                throw new LilyRTException("Error assigning Java service dependency " + dependencyName + " of module "
                        + springBuildContext.getModule().getDefinition().getId(), t);
            }

            RootBeanDefinition def = new RootBeanDefinition(ObjectFactoryBean.class);

            ConstructorArgumentValues args = new ConstructorArgumentValues();
            args.addIndexedArgumentValue(0, serviceClass);
            args.addIndexedArgumentValue(1, component);

            def.setConstructorArgumentValues(args);
            def.setLazyInit(false);
            parserContext.getRegistry().registerBeanDefinition(id, def);

            return null;
        }
    };

    private static final ElementProcessor EXPORT_SERVICE_PROCESSOR = new ElementProcessor() {
        public BeanDefinition process(Element element, ParserContext parserContext, SpringBuildContext springBuildContext)
                throws ClassNotFoundException {
            String service = element.getAttribute("service");
            Class serviceClass = parserContext.getReaderContext().getBeanClassLoader().loadClass(service);

            String beanName = element.getAttribute("ref");
            String name = element.getAttribute("name");
            if (name.equals("")) {
                name = beanName;
            }

            springBuildContext.exportJavaService(name, serviceClass, beanName);

            return null;
        }
    };

    private static final ElementProcessor CONF_PROCESSOR = new ElementProcessor() {
        public BeanDefinition process(Element element, ParserContext parserContext, SpringBuildContext springBuildContext)
                throws Exception {
            String path = element.getAttribute("path");
            String expr = element.getAttribute("select");
            String type = element.getAttribute("type");

            String moduleId = springBuildContext.getModule().getDefinition().getId();
            Conf conf = springBuildContext.getRuntime().getConfManager().getConfRegistry(moduleId).getConfiguration(path);

            if (expr.length() > 0) {
                JXPathContext context = JXPathContext.newContext(conf);
                Object value;

                try {
                    if (type != null && type.equals("node")) {
                        value = context.selectSingleNode(expr);
                        if (!(value instanceof Conf)) {
                            throw new LilyRTException("Element " + element.getTagName() + " of Spring bean config in module " +
                            moduleId + ": configuration pointed to by expression \"" + expr + "\""
                            + " does not evaluate to a node.");
                        }
                    } else {
                        value = String.valueOf(context.getValue(expr));
                    }
                } catch (LilyRTException e) {
                    throw e;
                } catch (Exception e) {
                    throw new LilyRTException("Element " + element.getTagName() + " of Spring bean config in module " +
                            moduleId + ": error retrieving configuration value using expression \"" + expr + "\""
                            + " from configuration path \"" + path + "\".", e);
                }

                GenericBeanDefinition def = new GenericBeanDefinition();
                def.setBeanClass(ObjectFactoryBean.class);

                ConstructorArgumentValues args = new ConstructorArgumentValues();
                args.addIndexedArgumentValue(0, value instanceof Conf ? Conf.class : String.class);
                args.addIndexedArgumentValue(1, value);

                def.setConstructorArgumentValues(args);
                def.setLazyInit(false);

                return def;
            } else {
                GenericBeanDefinition def = new GenericBeanDefinition();
                def.setBeanClass(ObjectFactoryBean.class);

                ConstructorArgumentValues args = new ConstructorArgumentValues();
                args.addIndexedArgumentValue(0, Conf.class);
                args.addIndexedArgumentValue(1, conf);

                def.setConstructorArgumentValues(args);
                def.setLazyInit(false);

                return def;
            }
        }
    };

    private static Map<String, ElementProcessor> ELEMENT_PROCESSORS;

    static {
        ELEMENT_PROCESSORS = new HashMap<String, ElementProcessor>();
        ELEMENT_PROCESSORS.put("import-service", IMPORT_SERVICE_PROCESSOR);
        ELEMENT_PROCESSORS.put("export-service", EXPORT_SERVICE_PROCESSOR);
        ELEMENT_PROCESSORS.put("module", MODULE_PROCESSOR);
        ELEMENT_PROCESSORS.put("conf", CONF_PROCESSOR);
    }
}
