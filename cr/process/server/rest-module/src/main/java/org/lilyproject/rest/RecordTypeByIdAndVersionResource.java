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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.SchemaId;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("schema/recordTypeById/{id}/version/{version:\\d+}")
public class RecordTypeByIdAndVersionResource extends TypeManagerEnabled {
    @GET
    @Produces("application/json")
    public Entity<RecordType> get(@PathParam("id") String id, @PathParam("version") Long version,
            @Context UriInfo uriInfo) {
        try {
            SchemaId schemaId = idGenerator.getSchemaId(id);
            RecordType recordType = typeManager.getRecordTypeById(schemaId, version);
            return Entity.create(recordType, uriInfo);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record type with id " + id + ", version " + version, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
