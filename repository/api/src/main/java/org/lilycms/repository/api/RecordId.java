/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.repository.api;

import java.util.SortedMap;

/**
 * Represents the id of a {@link Record}
 * 
 * <p> A string or byte[] representation can be requested of the Id.
 */
public interface RecordId {

    String toString();

    byte[] toBytes();

    /**
     * For variants, return the RecordId of the master record, for master records,
     * returns this.
     */
    RecordId getMaster();

    /**
     * For variants, returns the variant properties, for master records, returns
     * an empty map.
     */
    SortedMap<String, String> getVariantProperties();

    boolean equals(Object obj);
}
