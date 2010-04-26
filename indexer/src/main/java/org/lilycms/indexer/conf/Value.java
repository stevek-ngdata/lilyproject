package org.lilycms.indexer.conf;

import java.util.List;
import java.util.Set;

import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.exception.FieldNotFoundException;

public interface Value {
    List<String> eval(Record record, Repository repository, String vtag);

    QName getFieldDependency();

}
