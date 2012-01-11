/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.util.repo;

/**
 * RowLogContext is a context object to be used on a RowLogMessage.
 * <p>
 * It is intended to use from the HBaseRepository when calling processMessage
 * right after the message has been created and the repository still has a lock
 * on the record.
 * <p>
 * The RecordEvent and Record can be put on this context object to avoid that it
 * has to be constructed again when from the payload when processing the
 * message.
 */
public class RowLogContext {

    private RecordEvent recordEvent;

    public void setRecordEvent(RecordEvent recordEvent) {
        this.recordEvent = recordEvent;
    }

    public RecordEvent getRecordEvent() {
        return recordEvent;
    }
}
