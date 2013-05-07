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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.tools.restresourcegenerator.GenerateRepositoryResource;
import org.lilyproject.tools.restresourcegenerator.GenerateTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateRepositoryAndTableResource;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version/{version:\\d+}")
@GenerateTableResource
@GenerateRepositoryResource
@GenerateRepositoryAndTableResource
public class RecordByVersionResource extends BaseRepositoryResource {
    @GET
    @Produces("application/json")
    public Entity<Record> get(@PathParam("id") String id, @PathParam("version") Long version,
            @Context UriInfo uriInfo) {

        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        RecordId recordId = repository.getIdGenerator().fromString(id);

        try {
            return Entity.create(table.read(recordId, version), uriInfo);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (VersionNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, @PathParam("version") Long version, Record record,
            @Context UriInfo uriInfo) {

        RecordId recordId = getRepository(uriInfo).getIdGenerator().fromString(id);

        if (record.getId() != null && !record.getId().equals(recordId)) {
            throw new ResourceException("Record id in submitted record does not match record id in URI.",
                    BAD_REQUEST.getStatusCode());
        }

        if (record.getVersion() != null && !record.getVersion().equals(version)) {
            throw new ResourceException("Version in submitted record does not match version in URI.",
                    BAD_REQUEST.getStatusCode());
        }

        record.setId(recordId);
        record.setVersion(version);

        try {
            boolean useLatestRecordType = record.getRecordTypeName() == null || record.getRecordTypeVersion() == null;
            record = getTable(uriInfo).update(record, true, useLatestRecordType);
        } catch (RecordNotFoundException e) {
            throw new ResourceException("Record not found: " + recordId, NOT_FOUND.getStatusCode());
        } catch (VersionNotFoundException e) {
            throw new ResourceException("Record '" + recordId + "', version " + record.getVersion() + " not found.",
                    NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error updating versioned-mutable scope of record with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // TODO record we respond with should be full record or be limited to user-specified field list
        return Response.ok(Entity.create(record, uriInfo)).build();
    }
}
