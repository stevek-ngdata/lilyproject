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
     *
     * <p>Reserved characters, in the id itself or in the variant property key and value
     * should be escaped with backslash. Reserved characters are dot, comma and equals
     * (and backslash itself).</p>
     */
    String toString();

    /**
     * Returns the byte representation of this record id.
     *
     * <p>The bytes representation of record id's is designed such that they would provide
     * a meaningful row sort order in HBase, and be usable for scan operations. The encoding
     * is such that when an ID in un-encoded form is a prefix of another ID, it remains a
     * prefix when encoded as bytes. This allows for prefix-scanning a range of records.
     * (Of course, this only applies to user-specified IDs, not to UUID's).
     *
     * <p>The format for a master record id is as follows:
     * 
     * <pre>{identifier byte}{basic byte representation}</pre>
     *
     * <p>Where the identifier byte is (byte)0 for a USER record id, and (byte)1 for a UUID record id.
     *
     * <p>The {identifier byte} is put at the start because otherwise UUIDs
     * and USER-id's would be intermingled, preventing meaningful scan operations
     * on USER id's.</p>
     *
     * <p>In case there are variant properties:</p>
     *
     * <ul>
     *     <li>For USER record id's, a zero byte (NULL character) is appended to mark the end
     *     of the master record id. By consequence, use of the (non-printable) zero byte is
     *     forbidden in the master record id. The reason for choosing the zero byte is because
     *     it sorts before any other byte: this makes that the record id's of variants and
     *     their master will be sorted together, without any other master record in between.
     *     Any other master record id would have a byte larger than zero at that position.</li>
     *     <li>For UUID record id's, there is no separator byte between the master and the
     *     properties, since the UUID has a fixed length of 16 bytes. This also makes that
     *     the variant properties do not influence the sort order among the master record id's</li>
     * </ul>
     *
     * <p>The variant properties themselves are written as:
     *
     * <pre>({key string utf8 length}{key string in utf8}{value string utf8 length}{value string in utf8})*</pre>
     *
     * <p>There is no separator between the key-value pairs, as this is not needed. The key-value pairs are
     * always sorted by key.</p>
     *
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
