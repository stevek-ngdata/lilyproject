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
package org.lilycms.repository.impl.test;


import static org.easymock.classextension.EasyMock.createControl;

import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import static org.junit.Assert.*;
/**
 *
 */
public class FieldDescriptorImplTest {

    private IMocksControl control = createControl();

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        control.reset();
    }

    @Test
    public void testClone() {
        ValueType valueType = control.createMock(ValueType.class);
        control.replay();
        FieldDescriptor fieldDescriptor = new FieldDescriptorImpl("id", valueType, "name");
        assertEquals(fieldDescriptor, fieldDescriptor.clone());
        fieldDescriptor.setVersion(Long.valueOf(123));
        assertEquals(fieldDescriptor, fieldDescriptor.clone());
        control.verify();
    }
}
