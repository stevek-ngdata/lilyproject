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
package org.lilyproject.tools.recordrowvisualizer;

import org.joda.time.LocalDateTime;

public class RowLogKey implements Comparable<RowLogKey> {
    private long sequenceNr;
    private long timestamp;
    private long hbaseVersion;

    public RowLogKey(long sequenceNr, long timestamp, long hbaseVersion) {
        this.sequenceNr = sequenceNr;
        this.timestamp = timestamp;
        this.hbaseVersion = hbaseVersion;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTimestampFormatted() {
        return new LocalDateTime(timestamp).toString();
    }

    public long getHbaseVersion() {
        return hbaseVersion;
    }

    public String getHbaseVersionFormatted() {
        return new LocalDateTime(hbaseVersion).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RowLogKey other = (RowLogKey)obj;
        return other.sequenceNr == sequenceNr && other.timestamp == timestamp && other.hbaseVersion == hbaseVersion;
    }

    @Override
    public int hashCode() {
        return (int)(sequenceNr + hbaseVersion);
    }

    @Override
    public int compareTo(RowLogKey o) {
        if (sequenceNr < o.sequenceNr) {
            return -1;
        } else if (sequenceNr > o.sequenceNr) {
            return 1;
        }

        if (timestamp < o.timestamp) {
            return -1;
        } else if (timestamp > o.timestamp) {
            return 1;
        }

        if (hbaseVersion < o.hbaseVersion) {
            return -1;
        } else if (hbaseVersion > o.hbaseVersion) {
            return 1;
        }

        return 0;
    }
}
