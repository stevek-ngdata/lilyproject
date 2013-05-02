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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;

import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.tools.import_.core.ImportMode;
import org.lilyproject.tools.import_.core.ImportResult;
import org.lilyproject.tools.import_.core.ImportResultType;
import org.lilyproject.tools.import_.core.RecordImport;
import org.lilyproject.tools.restresourcegenerator.GenerateTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateTenantAndTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateTenantResource;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;

@Path("record/{id}")
@GenerateTableResource
@GenerateTenantResource
@GenerateTenantAndTableResource
public class RecordResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public Entity<Record> get(@PathParam("id") String id, @Context UriInfo uriInfo) {
        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        RecordId recordId = repository.getIdGenerator().fromString(id);
        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);
        try {
            return Entity.create(table.read(recordId, fieldQNames), uriInfo);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, Record record, @Context UriInfo uriInfo) {
        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        RecordId recordId = repository.getIdGenerator().fromString(id);

        if (record.getId() != null && !record.getId().equals(recordId)) {
            throw new ResourceException("Record id in submitted record does not match record id in URI.",
                    BAD_REQUEST.getStatusCode());
        }

        record.setId(recordId);

        ImportResult<Record> result;
        try {
            result = RecordImport.importRecord(record, ImportMode.CREATE_OR_UPDATE, table);
        } catch (Exception e) {
            throw new ResourceException("Error creating or updating record with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        // TODO record we respond with should be full record or be limited to user-specified field list
        record = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = uriInfo.getBaseUriBuilder().path(RecordResource.class).build(record.getId());
                response = Response.created(uri).entity(Entity.create(record, uriInfo)).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(Entity.create(record, uriInfo)).build();
                break;
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

    @POST
    @Produces("application/json")
    @Consumes("application/json")
    public Response post(@PathParam("id") String id, PostAction<Record> postAction, @Context UriInfo uriInfo) {
        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        if (postAction.getAction().equals("update")) {
            RecordId recordId = repository.getIdGenerator().fromString(id);
            Record record = postAction.getEntity();

            if (record.getId() != null && !record.getId().equals(recordId)) {
                throw new ResourceException("Record id in submitted record does not match record id in URI.",
                        BAD_REQUEST.getStatusCode());
            }

            record.setId(recordId);

            ImportResult<Record> result;
            try {
                result = RecordImport.importRecord(record, ImportMode.UPDATE, postAction.getConditions(), table);
            } catch (Exception e) {
                throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
            }

            // TODO record we respond with should be full record or be limited to user-specified field list
            record = result.getEntity();
            Response response;

            ImportResultType resultType = result.getResultType();
            switch (resultType) {
                case CANNOT_UPDATE_DOES_NOT_EXIST:
                    throw new ResourceException("Record not found: " + recordId, NOT_FOUND.getStatusCode());
                case UPDATED:
                case UP_TO_DATE:
                    response = Response.ok(Entity.create(record, uriInfo)).build();
                    break;
                case CONDITION_CONFLICT:
                    response = Response.status(CONFLICT.getStatusCode()).entity(Entity.create(record, uriInfo)).build();
                    break;
                default:
                    throw new RuntimeException("Unexpected import result type: " + resultType);
            }

            return response;

        } else if (postAction.getAction().equals("delete")) {
            RecordId recordId = repository.getIdGenerator().fromString(id);
            try {
                Record record = table.delete(recordId, postAction.getConditions());
                if (record != null && record.getResponseStatus() == ResponseStatus.CONFLICT) {
                    return Response.status(CONFLICT.getStatusCode()).entity(Entity.create(record, uriInfo)).build();
                }
            } catch (RecordNotFoundException e) {
                throw new ResourceException(e, NOT_FOUND.getStatusCode());
            } catch (Exception e) {
                throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
            }
            return Response.status(NO_CONTENT.getStatusCode()).build();
        } else {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }
    }

    @DELETE
    public Response delete(@PathParam("id") String id, @Context UriInfo uriInfo) {
        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        RecordId recordId = repository.getIdGenerator().fromString(id);
        try {
            table.delete(recordId);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return Response.status(NO_CONTENT.getStatusCode()).build();
    }
}
