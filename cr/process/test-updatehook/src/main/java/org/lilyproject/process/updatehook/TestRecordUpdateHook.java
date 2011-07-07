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
