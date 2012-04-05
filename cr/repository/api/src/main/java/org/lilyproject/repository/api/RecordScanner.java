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

import java.io.Closeable;
import java.io.IOException;

/**
 * A RecordScanner allows to sequentially run over all or part of the records
 * stored in the Lily repository.
 * 
 * <p>A scanner can be obtained from {@link Repository#getScanner(RecordScan)}</p>.
 * 
 * <p>RecordScanner's implement Iterable, so you can use a for-each loop
 * directly on them.</p>
 * 
 * <p>When done with a scanner, be sure to call {@link #close()} on them to release
 * the server-side resources.</p>
 */
public interface RecordScanner extends Closeable, Iterable<Record> {
    /**
     * Returns the next record, or null if there are none left.
     */
    Record next() throws RepositoryException, InterruptedException;

    /**
     * Closes this scanner, releasing its server-side resources.
     */
    void close();
}
