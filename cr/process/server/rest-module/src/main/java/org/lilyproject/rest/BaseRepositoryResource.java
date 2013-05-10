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

import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.util.exception.ExceptionUtil;
import org.springframework.beans.factory.annotation.Autowired;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class BaseRepositoryResource {
    protected RepositoryManager repositoryMgr;

    @Autowired
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryMgr = repositoryManager;
    }

    /**
     * Returns a repository based on the repositoryName path parameter.
     * <p>
     * This method discerns between classes marked with the {@link RepositoryEnabled} annotation and those without.
     * @param uriInfo context info from the incoming request
     * @return the appropriate repository (based on the repositoryName parameter, or the public repository)
     */
    protected LRepository getRepository(UriInfo uriInfo) {
        try {
            RepositoryEnabled repositoryEnabledAnnotation = getClass().getAnnotation(RepositoryEnabled.class);
            if (repositoryEnabledAnnotation != null &&
                    uriInfo.getPathParameters().containsKey(repositoryEnabledAnnotation.repositoryName())) {
                return repositoryMgr.getRepository(uriInfo.getPathParameters().getFirst(repositoryEnabledAnnotation.repositoryName()));
            } else {
                return repositoryMgr.getDefaultRepository();
            }
        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new ResourceException("Error retrieving repository", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    /**
     * Returns a table based on the tableName path parameter. The table is retrieved from the repository
     * determined by {@link #getRepository(javax.ws.rs.core.UriInfo)}.
     *
     * <p>
     * This method discerns between classes marked with the {@link TableEnabled} annotation and those without.
     * @param uriInfo context info from the incoming request
     * @return the appropriate table (based on the tableName parameter, or the default table)
     */
    protected LTable getTable(UriInfo uriInfo) {
        LRepository repository = getRepository(uriInfo);
        try {
            TableEnabled tableEnabledAnnotation = getClass().getAnnotation(TableEnabled.class);
            if (tableEnabledAnnotation != null) {
                return repository.getTable(uriInfo.getPathParameters().getFirst(tableEnabledAnnotation.tableName()));
            } else {
                return repository.getDefaultTable();
            }
        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new ResourceException("Error retrieving table", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
