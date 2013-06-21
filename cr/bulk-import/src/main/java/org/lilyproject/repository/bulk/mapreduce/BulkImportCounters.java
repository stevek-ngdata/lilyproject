/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.bulk.mapreduce;

/**
 * Enumeration of counters for bulk import metrics.
 */
public enum BulkImportCounters {

    /**
     * Total number of records that have been written.
     */
    OUTPUT_LILY_RECORDS,
    
    /**
     * Number of lines that didn't result in any output records being written.
     */
    INPUT_LINES_WITH_NO_OUTPUT,
    
    /**
     * Number of input lines that resulted in exactly one record.
     */
    INPUT_LINES_WITH_ONE_OUTPUT_RECORD,
    
    /**
     * Number of input lines that resulted in multiple output records.
     */
    INPUT_LINES_WITH_MULTIPLE_OUTPUT_RECORDS
}
