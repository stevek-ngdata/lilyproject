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
package org.lilyproject.repository.fake;

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.repository.api.TableManager;

public class FakeTableManager implements TableManager {
    @Override
    public RepositoryTable createTable(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RepositoryTable createTable(TableCreateDescriptor tableCreateDescriptor) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RepositoryTable> getTables() throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }
}
