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

import java.io.IOException;
import java.util.Collection;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexNotFoundException;
import org.lilyproject.indexer.model.api.IndexerModel;
import org.lilyproject.util.json.JsonFormat;
import org.restlet.representation.StringRepresentation;
import org.springframework.beans.factory.annotation.Autowired;

@Path("index")
public class IndexResource {
	
	@Autowired
	protected IndexerModel model;

    @GET
    @Produces("application/json")
    public Collection<IndexDefinition> get(@Context UriInfo uriInfo) {
        return model.getIndexes();
    }

    @GET
    @Path("{name}")
    @Produces("application/json")
    public IndexDefinition get(@PathParam("name") String name) throws IndexNotFoundException {
        return model.getIndex(name);
    }

    @GET
    @Path("{name}/config")
    @Produces("application/json")
    public Response getConfig(@PathParam("name") String name) throws IndexNotFoundException, IOException {
    	IndexDefinition index = model.getIndex(name);
    	
    	ObjectMapper m = new ObjectMapper();
    	ObjectNode json = m.createObjectNode();
    	json.put("zkDataVersion", index.getZkDataVersion());
    	json.put("config", index.getConfiguration());
		
    	return Response.ok(new StringRepresentation(new String(JsonFormat.serializeAsBytes(json)))).build();
    }

}
