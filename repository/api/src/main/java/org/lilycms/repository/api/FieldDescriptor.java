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
package org.lilycms.repository.api;

/**
 * Describes the properties of a field.
 * 
 * <p>Multiple versions of a {@link FieldDescriptor} can exist.
 * 
 * 
 */
public interface FieldDescriptor {
    /**
     * @return the id of the {@link Field}
     */
    String getFieldDescriptorId();
    
    /**
     * @return the version of the {
     */
    long getVersion();
    /**
     * return the type of the {@link Field}
     */
    String getFieldType();
    
    /**
     * @return if the {@link Field} is mandatory
     */
    boolean isMandatory();
    
    /**
     * @return if the {@link Field} is versionable
     */
    boolean isVersionable();
}
