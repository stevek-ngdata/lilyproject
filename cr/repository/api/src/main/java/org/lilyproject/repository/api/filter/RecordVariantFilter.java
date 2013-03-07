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

import java.util.Map;

import org.lilyproject.repository.api.RecordId;

/**
 * Filters based on variant properties. It only returns records which have exactly the same variant properties as the
 * given variant properties (i.e. not the records which have these variant properties and some additional ones). If the
 * value of the variant property is specified, it has to match exactly. If the value is <code>null</code>, any value
 * will match.
 *
 *
 */
public class RecordVariantFilter implements RecordFilter {

    private final RecordId masterRecordId;

    /**
     * Variant properties. Null values mean "any value", otherwise the value has to match exactly.
     */
    private final Map<String, String> variantProperties;

    public RecordVariantFilter(RecordId masterRecordId, Map<String, String> variantProperties) {
        this.masterRecordId = masterRecordId.getMaster();
        this.variantProperties = variantProperties;
    }

    public RecordId getMasterRecordId() {
        return masterRecordId;
    }

    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }
}
