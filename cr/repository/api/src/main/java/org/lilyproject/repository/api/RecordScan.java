package org.lilyproject.repository.api;

import org.lilyproject.repository.api.filter.RecordFilter;

/**
 * Defines the parameters of a scan to be performed over the records.
 * 
 * <p>See {@link Repository#getScanner(RecordScan)}</p>
 */
public class RecordScan {
    private RecordId startRecordId;
    private RecordId stopRecordId;
    private byte[] rawStartRecordId;
    private byte[] rawStopRecordId;
    private RecordFilter recordFilter;
    private ReturnFields returnFields;

    public RecordId getStartRecordId() {
        return startRecordId;
    }

    public void setStartRecordId(RecordId startRecordId) {
        this.startRecordId = startRecordId;
    }

    public RecordId getStopRecordId() {
        return stopRecordId;
    }

    /**
     * @param stopRecordId this is exclusive, scan stops at last entry before this id
     */
    public void setStopRecordId(RecordId stopRecordId) {
        this.stopRecordId = stopRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     */
    public byte[] getRawStartRecordId() {
        return rawStartRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     */
    public void setRawStartRecordId(byte[] rawStartRecordId) {
        this.rawStartRecordId = rawStartRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     */
    public byte[] getRawStopRecordId() {
        return rawStopRecordId;
    }

    /**
     * <b>EXPERT ONLY!</b>
     */
    public void setRawStopRecordId(byte[] rawStopRecordId) {
        this.rawStopRecordId = rawStopRecordId;
    }

    public RecordFilter getRecordFilter() {
        return recordFilter;
    }

    public void setRecordFilter(RecordFilter recordFilter) {
        this.recordFilter = recordFilter;
    }

    public ReturnFields getReturnFields() {
        return returnFields;
    }

    public void setReturnFields(ReturnFields returnFields) {
        this.returnFields = returnFields;
    }
}
