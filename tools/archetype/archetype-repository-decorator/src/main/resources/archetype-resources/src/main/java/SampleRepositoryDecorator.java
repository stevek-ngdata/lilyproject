package ${package};

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RepositoryDecorator;

import javax.annotation.PreDestroy;

public class SampleRepositoryDecorator extends BaseRepositoryDecorator {
    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RepositoryDecorator's */
    private String NAME = "${groupId}.${artifactId}";

    public SampleRepositoryDecorator(PluginRegistry pluginRegistry) {
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
        System.out.println("Before record create");
        record = super.create(record);
        System.out.println("After record create");
        return record;
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        System.out.println("Before record createOrUpdate");
        record = super.createOrUpdate(record);
        System.out.println("After record createOrUpdate");
        return record;
    }

    @Override
    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws RepositoryException, InterruptedException {
        System.out.println("Before record createOrUpdate");
        record = super.createOrUpdate(record, useLatestRecordType);
        System.out.println("After record createOrUpdate");
        return record;
    }


}
