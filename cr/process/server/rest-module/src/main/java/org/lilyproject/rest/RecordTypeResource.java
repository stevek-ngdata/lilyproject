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
import java.net.URI;

import org.apache.commons.lang.BooleanUtils;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.tools.import_.core.IdentificationMode;
import org.lilyproject.tools.import_.core.ImportMode;
import org.lilyproject.tools.import_.core.ImportResult;
import org.lilyproject.tools.import_.core.ImportResultType;
import org.lilyproject.tools.import_.core.RecordTypeImport;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("schema/recordType/{name}")
public class RecordTypeResource extends TypeManagerEnabled {
    @GET
    @Produces("application/json")
    public Entity<RecordType> get(@PathParam("name") String name, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return Entity.create(typeManager.getRecordTypeByName(qname, null), uriInfo);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record type with name " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("name") String name, RecordType recordType, @Context UriInfo uriInfo) {
        // Since the name can be updated, in this case we allow that the name in the submitted record type
        // is different from the name in the URI.
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());

        boolean refreshSubtypes = BooleanUtils.toBoolean(uriInfo.getQueryParameters().getFirst("refreshSubtypes"));

        ImportResult<RecordType> result;
        try {
            result = RecordTypeImport.importRecordType(recordType, ImportMode.CREATE_OR_UPDATE, IdentificationMode.NAME,
                    qname, refreshSubtypes, typeManager);
        } catch (Exception e) {
            throw new ResourceException("Error creating or updating record type named " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        recordType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = uriInfo.getBaseUriBuilder().path(RecordTypeResource.class).
                        queryParam("ns.n", recordType.getName().getNamespace()).
                        build("n$" + recordType.getName().getName());
                response = Response.created(uri).entity(Entity.create(recordType, uriInfo)).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                if (!recordType.getName().equals(qname)) {
                    // Reply with "301 Moved Permanently": see explanation in FieldTypeResource
                    uri = uriInfo.getBaseUriBuilder().path(RecordTypeResource.class).
                            queryParam("ns.n", recordType.getName().getNamespace()).
                            build("n$" + recordType.getName().getName());

                    return Response.status(Response.Status.MOVED_PERMANENTLY).header("Location", uri.toString()).
                            entity(Entity.create(recordType, uriInfo)).build();
                } else {
                    response = Response.ok(Entity.create(recordType, uriInfo)).build();
                }
                break;
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }
}
