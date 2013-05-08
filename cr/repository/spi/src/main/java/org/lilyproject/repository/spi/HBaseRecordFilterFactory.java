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
package org.lilyproject.repository.spi;

import org.apache.hadoop.hbase.filter.Filter;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;

/**
 * Implements the translation from a {@link RecordFilter} definition to an actual HBase filter
 * object.
 *
 * <p>Implementations are found through Java's {@link java.util.ServiceLoader}, thus by listing
 * their fully qualified class name in META-INF/services/org.lilyproject.repository.spi.HBaseRecordFilterFactory.</p>
 *
 * <p>Most of the time, implementations will work such that they filter out entire rows rather than
 * individual KeyValue's. In any case, implementations should never filter out Lily's system fields.</p>
 */
public interface HBaseRecordFilterFactory {
    /**
     * The implementation should check whether it recognizes the supplied RecordFilter implementation:
     * if not, it should return null.
     *
     * <p>The supplied repository object is mainly intended for TypeManager consultation.</p>
     *
     * @param factory to be used for creating nested filters
     */
    Filter createHBaseFilter(RecordFilter filter, LRepository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException;
}
