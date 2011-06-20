package org.lilyproject.process.decorator;

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RepositoryDecorator;

import javax.annotation.PreDestroy;

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
