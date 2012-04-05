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
package org.lilyproject.process.updatehook;

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.spi.RecordUpdateHook;

import javax.annotation.PreDestroy;

public class TestRecordUpdateHook implements RecordUpdateHook {
    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RecordUpdateHook's */
    private String NAME = "test-updatehook";

    public TestRecordUpdateHook(PluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        pluginRegistry.addPlugin(RecordUpdateHook.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RecordUpdateHook.class, NAME, this);
    }

    @Override
    public void beforeUpdate(Record record, Record originalRecord, Repository repository, FieldTypes fieldTypes)
            throws RepositoryException, InterruptedException {
        QName name = new QName("ns", "f1");
        String currentValue = (String)record.getField(name);
        record.setField(name, currentValue + "-hooked");
    }
}
