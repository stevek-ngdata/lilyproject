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
package org.lilyproject.repository.api;

import java.util.SortedMap;

import org.lilyproject.bytes.api.DataOutput;

/**
 * The id of a {@link Record}. This uniquely identifies a record.
 *
 * <p>A record ID consists of two parts: a mater record ID, and optionally variant properties.
 * 
 * <p>A string or byte[] representation can be requested of the Id.
 */
public interface RecordId {

    /**
     * Returns a string representation of this record id.
     * 
     * <p>The format for a master record id is as follows:
     *
     * <pre>{record id type}.{master record id}</pre>
     *
     * <p>Where the record id type is UUID or USER. For example:
     *
     * <pre>USER.2354236523</pre>
     *
     * <pre>A variant record id starts of the same, with the variant properties appended.
     *
     * <pre>{record id type}.{master record id}.varprop1=value1,varprop2=value2</pre>
     *
     * <p>The variant properties are sorted in lexicographic order.
     */
    String toString();

    /**
     * Returns the byte representation of this record id.
     * 
     * <p>The format for a master record id is as follows:
     * 
     * <pre>{basic byte representation}{identifier byte}</pre>
     * 
     * <p>Where the identifier byte is (byte)0 for a USER record id, and (byte)1 for a UUID record id.
     * 
     * <p>The format for a variant record id is as follows:
     * 
     * <pre>{master record id}{variant properties}{nr of variant properties}{length of master record id}{variant identifier byte}</pre>
     * 
     * <p>Where variant properties are UTF encodings for the dimension and dimension value of the variant properties (concatenated),
     * and the variant identifier byte is (byte)2 .
     * 
     * @return
     */
    byte[] toBytes();
    
    /**
     * Writes the bytes to the DataOutput with the same format as for {@link #toBytes()}
     */
    void writeBytes(DataOutput dataOutput);
    
    /**
     * For variants, return the RecordId of the master record, for master records,
     * returns this.
     */
    RecordId getMaster();

    /**
     * Returns true if this is the ID of a master record.
     */
    boolean isMaster();

    /**
     * For variants, returns the variant properties, for master records, returns
     * an empty map.
     */
    SortedMap<String, String> getVariantProperties();

    int hashCode();

    boolean equals(Object obj);
}
