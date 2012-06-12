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
package org.lilyproject.repository.api;

import org.lilyproject.repository.api.filter.RecordFilter;

/**
 * Defines the parameters of a scan to be performed over the records.
 *
 * <p>See {@link Repository#getScanner(RecordScan)}.</p>
 */
public class RecordScan {
    private RecordId startRecordId;
    private RecordId stopRecordId;
    private byte[] rawStartRecordId;
    private byte[] rawStopRecordId;
    private RecordFilter recordFilter;
    private ReturnFields returnFields;
    private int caching = -1;
    private boolean cacheBlocks = true;
    private boolean returnsIdRecords = false;

    /**
     * @see #setStartRecordId(RecordId)
     */
    public RecordId getStartRecordId() {
        return startRecordId;
    }

    /**
     * Sets the record ID where the scan should start.
     *
     * <p>If you don't set the start record ID, the scan will start at the very
     * first record. If you don't set a stop record ID either, then scan will
     * run until the last record. In such case, you are doing a full table scan.
     * Some filters (see {@link #setRecordFilter(RecordFilter)} are
     * also able to stop the scan early when certain conditions are reached,
     * see for example the {@link org.lilyproject.repository.api.filter.RecordIdPrefixFilter}.</p>
     *
     * <p>The start record ID does not have to be an existing record ID, the scan will
     * start from the first record which has a record ID greater than or equal to
     * this ID.</p>
     *
     * <p>It makes little sense to use this with UUID-based record ID's, except
     * if you would like to read all variants of a record. See also
     * {@link Repository#getVariants(RecordId)}, which only gives the ID's of the
     * variant records.</p>
     *
     * <p>To scan all rows starting with a common prefix, use the
     * {@link org.lilyproject.repository.api.filter.RecordIdPrefixFilter}</p>
     *
     * @param startRecordId record id to start on, inclusive
     * @see #setStopRecordId(RecordId)
     */
    public void setStartRecordId(RecordId startRecordId) {
        this.startRecordId = startRecordId;
    }

    /**
     * @see #setStopRecordId(RecordId)
     */
    public RecordId getStopRecordId() {
        return stopRecordId;
    }

    /**
     * Sets the record ID before which the scan should stop.
     *
     * <p>For more information, see {@link #setStartRecordId(RecordId)}.</p>
     *
     * @param stopRecordId record ID to stop on, exclusive (scan stops at last entry before this ID)
     */
    public void setStopRecordId(RecordId stopRecordId) {
        this.stopRecordId = stopRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     *
     * @see #setRawStartRecordId(byte[])
     */
    public byte[] getRawStartRecordId() {
        return rawStartRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b> Sets the start record ID as bytes.
     *
     * <p>If this is set, it takes precedence over {@link #setStartRecordId(RecordId)}.</p>
     *
     * <p>One case where this is used is for the MapReduce integration: the scans there
     * start/stop at the boundaries of a region, which can be defined as some
     * arbitrary byte sequence which is not necessarily a valid Lily record ID.</p>
     *
     * @see #setStartRecordId(RecordId)
     */
    public void setRawStartRecordId(byte[] rawStartRecordId) {
        this.rawStartRecordId = rawStartRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     *
     * @see #setRawStopRecordId(byte[])
     */
    public byte[] getRawStopRecordId() {
        return rawStopRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b> Sets the stop record ID as bytes.
     *
     * <p>If this is set, it takes precedence over {@link #setStopRecordId(RecordId)}.</p>
     *
     * @see #setStopRecordId(RecordId)
     */
    public void setRawStopRecordId(byte[] rawStopRecordId) {
        this.rawStopRecordId = rawStopRecordId;
    }

    /**
     * @see #setRecordFilter(RecordFilter)
     */
    public RecordFilter getRecordFilter() {
        return recordFilter;
    }

    /**
     * Sets a record filter. A record filter filters records server-side, without ever
     * returning them to the client.
     *
     * <p>There are various filter implementations available, check out the implementations
     * of {@link RecordFilter}.</p>
     *
     * <p>Note that filters do not work index-based: the scan will still run over every
     * record, evaluate the filter, and then decide to return the record or not.</p>
     *
     * <p>Multiple filters can be combined using
     * {@link org.lilyproject.repository.api.filter.RecordFilterList}.</p>
     */
    public void setRecordFilter(RecordFilter recordFilter) {
        this.recordFilter = recordFilter;
    }

    /**
     * @see #setReturnFields(ReturnFields)
     */
    public ReturnFields getReturnFields() {
        return returnFields;
    }

    /**
     * Sets the fields to return for each record.
     *
     * <p>By default, all fields of a record are returned. Limiting this to return only
     * the fields of interest can seriously speed up scan operations.</p>
     *
     * <p>You can either set {@link ReturnFields#ALL} and {@link ReturnFields#NONE} (if
     * you are only interested in record id and/or type), or a manual enumeration of
     * fields.</p>
     */
    public void setReturnFields(ReturnFields returnFields) {
        this.returnFields = returnFields;
    }

    /**
     * @see #setCaching(int)
     */
    public int getCaching() {
        return caching;
    }

    /**
     * Sets the number of records to be read in one go from the server.
     *
     * <p>By default caching is disabled, but it it is strongly recommended to enable it,
     * since otherwise each individual call to {@link RecordScanner#next()} will cause a
     * new request to be sent to the server.</p>
     */
    public void setCaching(int caching) {
        this.caching = caching;
    }

    /**
     * @see #setCacheBlocks(boolean)
     */
    public boolean getCacheBlocks() {
        return cacheBlocks;
    }

    /**
     * Enable or disable block caching. This sets whether the data loaded from disk by this scan
     * should be added to the server-side cache. By default this is true. It can be interesting
     * to disable this when you are doing a full table scan: otherwise this would in turn put
     * each loaded data block in the cache, possibly removing more frequently accessed data which
     * was already in there.
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    /**
     * @see #setReturnsIdRecords(boolean)
     */
    public boolean isReturnsIdRecords() {
        return returnsIdRecords;
    }

    /**
     * Enable or disable the scanner to return {@link IdRecord} instances in stead of {@link Record} instances. You
     * will
     * have to cast the results explicitely to {@link IdRecord} to make use of the additional {@link IdRecord}
     * features.
     *
     * <p>This is false by default, because it is usually not necessary and has a performance cost to it.</p>
     *
     * @see IdRecord for more information
     */
    public void setReturnsIdRecords(boolean returnsIdRecords) {
        this.returnsIdRecords = returnsIdRecords;
    }
}
