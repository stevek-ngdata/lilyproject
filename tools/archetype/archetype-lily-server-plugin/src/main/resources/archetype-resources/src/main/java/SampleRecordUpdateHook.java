package ${package};

import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.plugin.PluginRegistry;

import javax.annotation.PreDestroy;

public class SampleRecordUpdateHook implements RecordUpdateHook {
    private PluginRegistry pluginRegistry;
    /** Name should be unique among all RecordUpdateHook's */
    private String NAME = "${groupId}.${artifactId}";

    public SampleRecordUpdateHook(PluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
        pluginRegistry.addPlugin(RecordUpdateHook.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RecordUpdateHook.class, NAME, this);
    }

    @Override
    public void beforeUpdate(Record record, Record originalRecord, Repository repository, FieldTypes fieldTypes,
            RecordEvent recordEvent) throws RepositoryException, InterruptedException {
        System.out.println("Record update hook is called for record: " + record.getId());
    }

    @Override
    public void beforeCreate(Record newRecord, Repository repository, FieldTypes fieldTypes, RecordEvent recordEvent)
            throws RepositoryException, InterruptedException {
        System.out.println("Record update hook is called for record: " + record.getId());
    }

    @Override
    public void beforeDelete(Record originalRecord, Repository repository, FieldTypes fieldTypes,
            RecordEvent recordEvent) throws RepositoryException, InterruptedException {
        System.out.println("Record update hook is called for record: " + record.getId());
    }
}