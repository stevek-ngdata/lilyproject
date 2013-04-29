/*
 * Copyright 2013 NGDATA nv
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

import org.junit.Test;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PureRepositoryTest {
    @Test
    public void test() throws Exception {
        Repository repository = mock(Repository.class);
        RecordId recordId = mock(RecordId.class);
        Record record = mock(Record.class);

        when(repository.read(recordId)).thenReturn(record);

        Repository pureRepository = PureRepository.wrap(repository);

        try {
            pureRepository.read((RecordId)null);
            fail("expected exception");
        } catch (IllegalStateException e) {
            // ok
        }
    }
}
