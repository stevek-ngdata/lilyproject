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
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RepositoryDecoratorFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRepositoryDecoratorFactory implements RepositoryDecoratorFactory {
    public static AtomicInteger CLOSE_COUNT = new AtomicInteger();

    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RepositoryDecoratorFactory's */
    public static final String NAME = "test-decorator";

    public TestRepositoryDecoratorFactory(PluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        pluginRegistry.addPlugin(RepositoryDecoratorFactory.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RepositoryDecoratorFactory.class, NAME, this);
    }

    @Override
    public Repository createInstance(Repository delegate) {
        return new TestRepositoryDecorator(delegate);
    }

    public class TestRepositoryDecorator extends BaseRepositoryDecorator {
        public TestRepositoryDecorator(Repository delegate) {
            super(delegate);
        }

        @Override
        public Record create(Record record) throws RepositoryException, InterruptedException {
            record.setField(new QName("ns", "f2"), "foo");
            record = super.create(record);
            return record;
        }

        @Override
        public void close() throws IOException {
            CLOSE_COUNT.incrementAndGet();
        }
    }

}
