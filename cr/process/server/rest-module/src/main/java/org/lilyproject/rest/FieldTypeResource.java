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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.tools.import_.core.FieldTypeImport;
import org.lilyproject.tools.import_.core.IdentificationMode;
import org.lilyproject.tools.import_.core.ImportMode;
import org.lilyproject.tools.import_.core.ImportResult;
import org.lilyproject.tools.import_.core.ImportResultType;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("schema/fieldType/{name}")
public class FieldTypeResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public Entity<FieldType> get(@PathParam("name") String name, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return Entity.create(repository.getTypeManager().getFieldTypeByName(qname));
        } catch (FieldTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading field type with name " + qname, e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("name") String name, FieldType fieldType, @Context UriInfo uriInfo) {
        // Since the name can be updated, in this case we allow that the name in the submitted field type
        // is different from the name in the URI.
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());

        ImportResult<FieldType> result;
        try {
            result = FieldTypeImport.importFieldType(fieldType, ImportMode.CREATE_OR_UPDATE, IdentificationMode.NAME,
                    qname, repository.getTypeManager());
        } catch (Exception e) {
            throw new ResourceException("Error creating or updating field type named " + qname, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        fieldType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case CREATED:
                URI uri = UriBuilder.fromResource(FieldTypeResource.class).
                        queryParam("ns.n", fieldType.getName().getNamespace()).
                        build("n$" + fieldType.getName().getName());
                response = Response.created(uri).entity(Entity.create(fieldType)).build();
                break;
            case UPDATED:
            case UP_TO_DATE:
                // About the use of "301 Moved Permanently" rather than "200 OK": I'm following here page
                // 198 of "RESTful Web Services" (Richardson & Ruby):
                //   The exception is if the client successfully changes a user's name. Now that resource is
                //   available under a different URI: say, /users/lenoard instead of /users/leonardr. That
                //   means I need to send a response code of 301 ("Moved Permanently") and put the user's
                //   new URI in the Location header.
                if (!fieldType.getName().equals(qname)) {
                    uri = UriBuilder.fromResource(FieldTypeResource.class).
                            queryParam("ns.n", fieldType.getName().getNamespace()).
                            build("n$" + fieldType.getName().getName());

                    return Response.status(Response.Status.MOVED_PERMANENTLY).header("Location", uri).
                            entity(Entity.create(fieldType)).build();
                } else {
                    response = Response.ok(Entity.create(fieldType)).build();
                }
                break;
            case CONFLICT:
                throw new ResourceException(String.format("Field type %1$s exists but with %2$s %3$s instead of %4$s",
                        qname, result.getConflictingProperty(), result.getConflictingOldValue(),
                        result.getConflictingNewValue()), CONFLICT.getStatusCode());
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

}
