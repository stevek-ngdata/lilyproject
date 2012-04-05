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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.conf.Conf;
import org.lilyproject.plugin.PluginHandle;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.plugin.PluginUser;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.spi.RepositoryDecorator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies all the RepositoryDecorators to the Repository.
 */
public class RepositoryDecoratorActivator implements PluginUser<RepositoryDecorator> {
    private PluginRegistry pluginRegistry;
    private Repository repository;
    private Repository decoratedRepository;
    private Map<String, RepositoryDecorator> decorators = new HashMap<String, RepositoryDecorator>();
    private List<String> configuredDecorators = new ArrayList<String>();
    private Log log = LogFactory.getLog(getClass());

    public RepositoryDecoratorActivator(PluginRegistry pluginRegistry, Repository repository, Conf conf) {
        this.pluginRegistry = pluginRegistry;
        this.repository = repository;

        for (Conf decoratorConf : conf.getChild("decorators").getChildren("decorator")) {
            configuredDecorators.add(decoratorConf.getValue());
        }
    }

    @PostConstruct
    public void init() {
        pluginRegistry.setPluginUser(RepositoryDecorator.class, this);
    }

    @PreDestroy
    public void destroy() {
        pluginRegistry.unsetPluginUser(RepositoryDecorator.class, this);
    }

    @Override
    public void pluginAdded(PluginHandle<RepositoryDecorator> pluginHandle) {
        decorators.put(pluginHandle.getName(), pluginHandle.getPlugin());
    }

    @Override
    public void pluginRemoved(PluginHandle<RepositoryDecorator> pluginHandle) {
    }

    public Repository getDecoratedRepository() {
        // We connect once at startup the decorators for the repository. These should be added before
        // the first use of the repository, we don't allow changes to the decorators afterwards.
        if (decoratedRepository == null) {
            // We don't use all the registered decorator plugins, but only those the user
            // activated through the configuration, and in the order specified in the
            // configuration
            List<RepositoryDecorator> decorators = new ArrayList<RepositoryDecorator>(configuredDecorators.size());
            for (String name : configuredDecorators) {
                RepositoryDecorator decorator = this.decorators.get(name);
                if (decorator == null) {
                    throw new RuntimeException("No repository decorator registered with the name '" + name + "'");
                }
                decorators.add(decorator);
            }

            log.info("The active repository decorators are: " + configuredDecorators);

            //
            // Now connect the decorators
            //
            Repository next = repository;

            for (int i = decorators.size() - 1; i >= 0; i--) {
                decorators.get(i).setDelegate(next);
                next = decorators.get(i);
            }

            decoratedRepository = decorators.size() == 0 ? repository : decorators.get(0);
        }
        return decoratedRepository;
    }

}
