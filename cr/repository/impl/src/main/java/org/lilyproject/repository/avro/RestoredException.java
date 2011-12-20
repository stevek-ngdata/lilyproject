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
package org.lilyproject.repository.avro;

import org.lilyproject.util.exception.RemoteThrowableInfo;

import java.util.List;

public class RestoredException extends Exception implements RemoteThrowableInfo {
    private String originalClass;
    private String originalMessage;
    private List<StackTraceElement> stackTrace;

    public RestoredException(String message, String originalClass, List<StackTraceElement> stackTrace) {
        super("[remote exception of type " + originalClass + "]" + message);
        this.originalMessage = message;
        this.originalClass = originalClass;
        this.stackTrace = stackTrace;
    }

    @Override
    public String getOriginalClass() {
        return originalClass;
    }

    @Override
    public List<StackTraceElement> getOriginalStackTrace() {
        return stackTrace;
    }

    @Override
    public String getOriginalMessage() {
        return originalMessage;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append((originalMessage != null) ? (originalClass + ": " + originalMessage) : originalClass);

        if (stackTrace != null) {
            builder.append("\n\tWARNING: This is reproduced information of a remote exception.");
            builder.append("\n\t         This exception did not occur in this JVM!");
            for (StackTraceElement aTrace : stackTrace)
                builder.append("\n\tat ").append(aTrace);
            builder.append("\n");
        }

        return builder.toString();
    }
}
