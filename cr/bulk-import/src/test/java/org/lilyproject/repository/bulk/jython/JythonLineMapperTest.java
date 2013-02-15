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
package org.lilyproject.repository.bulk.jython;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.lilyproject.repository.bulk.LineMappingContext;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.bulk.LineMapper;

public class JythonLineMapperTest {
    
    private static String pythonCode;
    private LineMappingContext mappingContext;
    private Record record;
    private RecordId recordId;
    
    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        pythonCode = Resources.toString(Resources.getResource("mappers.py"), Charsets.UTF_8);
    }

    @Before
    public void setUp() {
        mappingContext = mock(LineMappingContext.class);
        record = mock(Record.class);
        recordId = mock(RecordId.class);
        when(mappingContext.newRecord()).thenReturn(record);
        when(mappingContext.newRecordId()).thenReturn(recordId);
    }

    @Test
    public void testMap_SimpleCase() throws RecordException, IOException, InterruptedException {
        LineMapper lineMapper = new JythonLineMapper(pythonCode, "singleFieldRecord");
        
        QName fieldName = QName.fromString("{org.lilyproject}Name");
        QName recordTypeName = QName.fromString("{org.lilyproject}NameRecord");
        
        when(mappingContext.qn("{org.lilyproject}Name")).thenReturn(fieldName);
        when(mappingContext.qn("{org.lilyproject}NameRecord")).thenReturn(recordTypeName);
        
        lineMapper.mapLine("nameValue", mappingContext);
        
        verify(record).setField(fieldName, "nameValue");
        verify(record).setRecordType(recordTypeName);
        verify(mappingContext).writeRecord(record);
    }
    
    @Test
    public void testMap_SimpleCase_WithInstance() throws RecordException, IOException, InterruptedException {
        LineMapper lineMapper = new JythonLineMapper(pythonCode, "mapperInstanceMethod");
        
        QName fieldName = QName.fromString("{org.lilyproject}Name");
        QName recordTypeName = QName.fromString("{org.lilyproject}NameRecord");
        
        when(mappingContext.qn("{org.lilyproject}Name")).thenReturn(fieldName);
        when(mappingContext.qn("{org.lilyproject}NameRecord")).thenReturn(recordTypeName);
        
        lineMapper.mapLine("nameValue", mappingContext);
        
        verify(record).setField(fieldName, "nameValue");
        verify(record).setRecordType(recordTypeName);
        verify(mappingContext).writeRecord(record);
    }
    
    @Test
    public void testMap_NullFromJythonMapper() throws IOException, InterruptedException {
        LineMapper lineMapper = new JythonLineMapper(pythonCode, "mapsNothing");
        lineMapper.mapLine("inputValue", mappingContext);
        verify(mappingContext, never()).writeRecord(any(Record.class));
    }

    @Test(expected=Exception.class)
    public void testInstantiate_UndefinedSymbol() {
        new JythonLineMapper(pythonCode, "doesNotExist");
    }
    
}
