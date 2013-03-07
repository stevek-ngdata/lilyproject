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
package org.lilyproject.process.decorator;

import javax.annotation.PreDestroy;

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RepositoryDecorator;

public class TestRepositoryDecorator extends BaseRepositoryDecorator {
    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RepositoryDecorator's */
    private String NAME = "test-decorator";

    public TestRepositoryDecorator(PluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        pluginRegistry.addPlugin(RepositoryDecorator.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RepositoryDecorator.class, NAME, this);
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        record = super.create(record);
        record.setField(new QName("decorator-test", "post-create"), "yes");
        return record;
    }
}
