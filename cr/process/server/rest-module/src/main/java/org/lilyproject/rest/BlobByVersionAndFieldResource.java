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

import org.lilyproject.repository.api.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version/{version:\\d+}/field/{fieldName}/data")
public class BlobByVersionAndFieldResource extends RepositoryEnabled {

    @GET
    @Produces("*/*")
    public Response get(@PathParam("id") String id, @PathParam("version") String version,
            @PathParam("fieldName") String fieldName, @Context UriInfo uriInfo) {
        return getBlob(id, version, fieldName, uriInfo, repository);
    }


    protected static Response getBlob(String id, String version, String fieldName, UriInfo uriInfo,
            final Repository repository) {
        final RecordId recordId = repository.getIdGenerator().fromString(id);

        final QName fieldQName = ResourceClassUtil.parseQName(fieldName, uriInfo.getQueryParameters());

        Long versionNr = null;
        if (version != null) {
            versionNr = Long.parseLong(version);
        }

        try {
            final BlobAccess blobAccess = repository.getBlob(recordId, versionNr, fieldQName, null, null);
            return Response.ok(blobAccess, MediaType.valueOf(blobAccess.getBlob().getMediaType())).build();
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (FieldNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (BlobNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
