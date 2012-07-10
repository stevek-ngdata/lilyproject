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
package org.lilyproject.indexer.model.util;

import org.lilyproject.repository.api.QName;

import java.util.Collection;
import java.util.Set;

/**
 * Utility bean providing access to the parsed indexerconfs for all defined indexes. It keeps
 * these permanently available for fast access, and updates them when the indexer model changes.
 */
public interface IndexesInfo {
    Collection<IndexInfo> getIndexInfos();

    Set<QName> getRecordFilterFieldDependencies();

    boolean getRecordFilterDependsOnRecordType();
}
