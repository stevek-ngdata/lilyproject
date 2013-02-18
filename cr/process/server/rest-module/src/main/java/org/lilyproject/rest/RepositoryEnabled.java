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
package org.lilyproject.rest;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.springframework.beans.factory.annotation.Autowired;

public class RepositoryEnabled {
    protected RepositoryManager repositoryMgr;

    @Autowired
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryMgr = repositoryManager;
    }
    
    public Repository getRepository() {
        try {
            return repositoryMgr.getRepository(Table.RECORD.name);
        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new ResourceException("Error retrieving repository", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
