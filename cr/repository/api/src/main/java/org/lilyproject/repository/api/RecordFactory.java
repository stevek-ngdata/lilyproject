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

/**
 * Creates records which can be written to a {@link Repository}. Created records have no persistence until they have
 * been written to a {@code Repository}.
 */
public interface RecordFactory {

    /**
     * Instantiates a new Record object.
     * 
     * <p>
     * This is only a factory method, nothing is created in the repository.
     */
    Record newRecord() throws RecordException;

    /**
     * Instantiates a new Record object with the RecordId already filled in.
     * 
     * <p>
     * This is only a factory method, nothing is created in the repository.
     */
    Record newRecord(RecordId recordId) throws RecordException;

}
