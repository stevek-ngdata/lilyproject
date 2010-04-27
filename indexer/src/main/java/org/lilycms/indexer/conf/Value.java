package org.lilycms.indexer.conf;

import java.util.List;
import java.util.Set;

import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.exception.FieldNotFoundException;

public interface Value {
    List<String> eval(Record record, Repository repository, String vtag);

    /**
     * Returns the field that is used from the record when evaluating this value. It is the value that is taken
     * from the current record, thus in the case of a dereference it is the first link field, not the field value
     * taken from the target document.
     */
    QName getFieldDependency();    

}
