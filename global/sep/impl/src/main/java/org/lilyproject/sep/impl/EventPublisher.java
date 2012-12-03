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
package org.lilyproject.sep.impl;

import java.io.IOException;

/**
 * Publisher of secondary events which are distributed to and handled by {@link EventListener}s.
 */
public interface EventPublisher {

    /**
     * Publish a message to be processed by the secondary event processor system.
     * 
     * @param row The row key for the record to which the message is related
     * @param payload The content of the event message
     * @return true if the message was successfully published, false otherwise
     */
    boolean publishMessage(byte[] row, byte[] payload) throws IOException;
}
