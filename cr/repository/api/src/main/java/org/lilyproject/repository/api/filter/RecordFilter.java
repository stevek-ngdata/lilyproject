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
package org.lilyproject.repository.api.filter;

/**
 * A RecordFilter filters records when performing scan operations.
 * 
 * <p>To use filters, find an implementation of this interface and supply an instance of it to
 * {@link org.lilyproject.repository.api.RecordScan#setRecordFilter(RecordFilter)}</p>
 *
 * <p>Classes extending from this interface are the specification (definition) of a filter:
 * the type of the filter with its various parameters.</p>
 *
 * <p>RecordFilters are translated to HBase filters, so they are evaluated within the
 * HBase region servers.</p>
 *
 * <p>The translation to HBase filters is performed by classes implementing the interface
 * HBaseRecordFilterFactory which is part of lily-repository-spi.</p>
 *
 */
public interface RecordFilter {
}
