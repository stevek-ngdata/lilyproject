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
package org.lilyproject.repository.api;


/**
 * A ValueTypeFactory is used to create a new instance of a {@link ValueType}.
 * 
 * Each ValueType should have a corresponding ValueTypeFactory and it should be 
 * registered on the TypeManager through {@link TypeManager#registerValueType(String, ValueTypeFactory)}
 * 
 */
public interface ValueTypeFactory {

    /**
     *  Returns an instance of the ValueType
     *  @param typeParams the value type specific parameters as String 
     */
    ValueType getValueType(String typeParams) throws RepositoryException, InterruptedException;
}
