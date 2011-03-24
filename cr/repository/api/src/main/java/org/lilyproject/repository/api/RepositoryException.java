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

import java.util.Map;

/**
 * This exception encapsulates any lower-level exceptions thrown by the repository.
 * These exceptions should not occur under normal operation of the repository.
 */
public class RepositoryException extends Exception {
    /**
     * Constructor only used by {@link AvroConverter} to reconstruct the original exception.
     * 
     * <p>Each subclass should implement this constructor and be able to re-construct itself based on either the message or the state.
     * 
     * @param message the message of the exception
     * @param state the other values of the exception
     */
    public RepositoryException(String message, Map<String, String> state) {
        super(message);
    }

    /**
     * Returns a map with the state of the exception so that its constructor can use them to recreate the original exception.
     * 
     * <p>Each subclass should implement this method.
     * <p>Only to be used by the {@link AvroConverter}
     */
    public Map<String, String> getState() {
        return null;
    }
    
    public RepositoryException() {
        super();
    }

    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryException(Throwable cause) {
        super(cause);
    }
    
}
