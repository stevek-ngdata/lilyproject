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
package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.repo.SystemFields;

public interface IndexUpdateBuilder {

    public void addField(String fieldName, List<String> value) throws InterruptedException, RepositoryException;

    public LRepository getRepository();

    public SystemFields getSystemFields();

    public RecordContext getRecordContext();

    public void push(Record record, Dep dep);

    public void push(Record record, Record contextRecord, Dep dep);

    public RecordContext pop();

    public void addDependency(SchemaId field);

    public List<String> eval(Value value) throws RepositoryException, IOException, InterruptedException;

    public NameTemplateResolver getFieldNameResolver();

    public SchemaId getVTag();

    public String evalIndexFieldName(NameTemplate nameTemplate);

    /**
     * Returns the name of the repository table where the indexed record resides.
     */
    public String getTable();

}
