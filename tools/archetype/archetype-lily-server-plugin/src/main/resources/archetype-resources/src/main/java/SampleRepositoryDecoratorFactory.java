package ${package};

import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.spi.BaseRepositoryDecorator;
import org.lilyproject.repository.spi.RepositoryDecoratorFactory;

import javax.annotation.PreDestroy;

public class SampleRepositoryDecoratorFactory implements RepositoryDecoratorFactory {
    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RepositoryDecorator's */
    private String NAME = "${groupId}.${artifactId}";

    public SampleRepositoryDecoratorFactory(PluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        pluginRegistry.addPlugin(RepositoryDecoratorFactory.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RepositoryDecoratorFactory.class, NAME, this);
    }

    public Repository createInstance(Repository delegate) {
        System.out.println("Creating a decorator for repository " + delegate.getRepositoryName()
                + ", table " + delegate.getTableName());
        return new SampleRepositoryDecorator(delegate);
    }

    public static class SampleRepositoryDecorator extends BaseRepositoryDecorator {

        public SampleRepositoryDecorator(Repository delegate) {
            super(delegate);
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
}
