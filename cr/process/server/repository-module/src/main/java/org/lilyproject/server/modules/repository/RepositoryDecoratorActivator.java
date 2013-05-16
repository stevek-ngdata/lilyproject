/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.server.modules.repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.plugin.PluginHandle;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.plugin.PluginUser;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.spi.RepositoryDecoratorFactory;
import org.lilyproject.runtime.conf.Conf;

/**
 * Applies all the RepositoryDecorators to a Repository.
 */
public class RepositoryDecoratorActivator implements PluginUser<RepositoryDecoratorFactory> {
    private PluginRegistry pluginRegistry;
    private Map<String, RepositoryDecoratorFactory> decorators = new HashMap<String, RepositoryDecoratorFactory>();
    private List<String> configuredDecorators = new ArrayList<String>();
    private Log log = LogFactory.getLog(getClass());

    public RepositoryDecoratorActivator(PluginRegistry pluginRegistry, Conf conf) {
        this.pluginRegistry = pluginRegistry;

        for (Conf decoratorConf : conf.getChild("decorators").getChildren("decorator")) {
            configuredDecorators.add(decoratorConf.getValue());
        }
    }

    @PostConstruct
    public void init() {
        pluginRegistry.setPluginUser(RepositoryDecoratorFactory.class, this);
    }

    @PreDestroy
    public void destroy() {
        pluginRegistry.unsetPluginUser(RepositoryDecoratorFactory.class, this);
    }

    @Override
    public void pluginAdded(PluginHandle<RepositoryDecoratorFactory> pluginHandle) {
        decorators.put(pluginHandle.getName(), pluginHandle.getPlugin());
    }

    @Override
    public void pluginRemoved(PluginHandle<RepositoryDecoratorFactory> pluginHandle) {
    }

    public RepositoryDecoratorChain getDecoratedRepository(Repository repository) {
        // We don't use all the registered decorator plugins, but only those the user
        // activated through the configuration, and in the order specified in the
        // configuration

        log.info("The active repository decorators are: " + configuredDecorators);

        RepositoryDecoratorChain chain = new RepositoryDecoratorChain();

        Repository nextInChain = repository;
        chain.addEntryAtStart(RepositoryDecoratorChain.UNDECORATED_REPOSITORY_KEY, nextInChain);

        for (int i = configuredDecorators.size() - 1; i >=0; i--) {
            String decoratorName = configuredDecorators.get(i);
            RepositoryDecoratorFactory factory = decorators.get(decoratorName);
            if (factory == null) {
                throw new RuntimeException("No repository decorator registered with the name '" + decoratorName + "'");
            }
            nextInChain = factory.createInstance(nextInChain);
            chain.addEntryAtStart(decoratorName, nextInChain);
        }

        return chain;
    }

}
