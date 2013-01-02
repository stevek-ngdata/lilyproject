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

import org.lilyproject.repository.api.RecordId;

/**
 * A filter which lets through records whose ID starts with the given RecordId.
 * <p/>
 * <p>It does not make sense to use this with UUID-based record IDs.</p>
 * <p/>
 * <p>When using this filter, you should set
 * {@link org.lilyproject.repository.api.RecordScan#setStartRecordId(org.lilyproject.repository.api.RecordId)}
 * to the same RecordId as set here, since any earlier records will be dropped
 * anyway by this filter.</p>
 * <p/>
 * <p>The scan will stop as soon as a record ID is encountered which is larger than
 * what can be matched by the prefix. So the scan will not needlessly run to the end
 * of the table.</p>
 */
public class RecordIdPrefixFilter implements RecordFilter {
    private RecordId recordId;

    public RecordIdPrefixFilter() {
    }

    public RecordIdPrefixFilter(RecordId recordId) {
        this.recordId = recordId;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }
}
