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
package org.lilyproject.rest.index;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Splitter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.indexer.model.api.IndexBatchBuildState;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexGeneralState;
import org.lilyproject.indexer.model.api.IndexNotFoundException;
import org.lilyproject.indexer.model.api.IndexUpdateState;
import org.lilyproject.indexer.model.api.WriteableIndexerModel;
import org.lilyproject.rest.RepositoryEnabled;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.util.ObjectUtils;
import org.lilyproject.util.json.JsonFormat;
import org.restlet.representation.StringRepresentation;
import org.springframework.beans.factory.annotation.Autowired;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("")
public class IndexResource extends RepositoryEnabled {

    @Autowired
    protected WriteableIndexerModel model;

    @Autowired
    private Indexer indexer;

    /**
     * Get all index definitions.
     */
    @GET
    @Produces("application/json")
    public Collection<IndexDefinition> get(@Context UriInfo uriInfo) {
        return model.getIndexes();
    }

    /**
     * Get a single index definition.
     */
    @GET
    @Path("{name}")
    @Produces("application/json")
    public IndexDefinition get(@PathParam("name") String name) throws IndexNotFoundException {
        return model.getIndex(name);
    }

    /**
     * Get a single index configuration (as stored in zookeeper).
     */
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

    /**
     * Update an index definition.
     */
    @PUT
    @Path("{name}")
    @Consumes("application/json")
    @Produces("application/json")
    public IndexDefinition put(@PathParam("name") String indexName, ObjectNode json) throws Exception {
        IndexDefinition index = model.getMutableIndex(indexName);

        IndexGeneralState generalState = json.has("generalState") ? IndexGeneralState.valueOf(json.get("generalState")
                .getTextValue()) : null;
        IndexUpdateState updateState = json.has("updateState") ? IndexUpdateState.valueOf(json.get("updateState")
                .getTextValue()) : null;
        IndexBatchBuildState buildState =
                json.has("batchBuildState") ? IndexBatchBuildState.valueOf(json.get("batchBuildState")
                        .getTextValue()) : null;
        // adding this for backwards compatibility.
        buildState = json.has("buildState") && buildState == null ? IndexBatchBuildState.valueOf(json.get("buildState")
                .getTextValue()) : buildState;


        byte[] defaultBatchIndexConfiguration = json.has("defaultBatchIndexConfiguration") ?
                JsonFormat.serializeAsBytes(json.get("defaultBatchIndexConfiguration")) : null;
        byte[] batchIndexConfiguration = json.has("batchIndexConfiguration") ?
                JsonFormat.serializeAsBytes(json.get("batchIndexConfiguration")) : null;

        String lock = model.lockIndex(indexName);
        try {

            boolean changes = false;

            if (generalState != null && generalState != index.getGeneralState()) {
                index.setGeneralState(generalState);
                changes = true;
            }

            if (updateState != null && updateState != index.getUpdateState()) {
                index.setUpdateState(updateState);
                changes = true;
            }

            if (buildState != null && buildState != index.getBatchBuildState()) {
                index.setBatchBuildState(buildState);
                changes = true;
            }

            if (json.has("defaultBatchIndexConfiguration") && !ObjectUtils.safeEquals(defaultBatchIndexConfiguration,
                    index.getDefaultBatchIndexConfiguration())) {
                index.setDefaultBatchIndexConfiguration(defaultBatchIndexConfiguration);
                changes = true;
            }

            if (batchIndexConfiguration != null) {
                index.setBatchIndexConfiguration(batchIndexConfiguration);
                changes = true;
            }

            if (changes) {
                model.updateIndex(index, lock);
                //System.out.println("Index updated: " + indexName);
            } else {
                //System.out.println("Index already matches the specified settings, did not update it.");
            }


        } finally {
            // In case we requested deletion of an index, it might be that the lock is already removed
            // by the time we get here as part of the index deletion.
            boolean ignoreMissing = generalState != null && generalState == IndexGeneralState.DELETE_REQUESTED;
            model.unlockIndex(lock, ignoreMissing);
        }

        return index;
    }

    /**
     * Trigger indexing of a record on the specified index.
     */
    @POST
    @Path("{name}")
    public void indexOn(@QueryParam("action") String action, @PathParam("name") String indexName,
                        @QueryParam("id") String recordId) throws Exception {
        if ("index".equals(action)) {
            indexer.indexOn(repository.getIdGenerator().fromString(recordId),
                    new HashSet<String>(Arrays.asList(indexName)));
        } else {
            throw new ResourceException("Unsupported POST action: " + action, BAD_REQUEST.getStatusCode());
        }
    }

    /**
     * Trigger indexing of a record on specified or all matching indexes.
     */
    @POST
    @Path("")
    public void index(@QueryParam("action") String action, @QueryParam("indexes") String commaSeparatedIndexNames,
                      @QueryParam("id") String recordId) throws Exception {
        if ("index".equals(action)) {
            final Set<String> indexNames = parse(commaSeparatedIndexNames);
            if (indexNames.isEmpty()) {
                indexer.index(repository.getIdGenerator().fromString(recordId));
            } else {
                indexer.indexOn(repository.getIdGenerator().fromString(recordId), indexNames);
            }
        } else {
            throw new ResourceException("Unsupported POST action: " + action, BAD_REQUEST.getStatusCode());
        }
    }

    private Set<String> parse(String commaSeparatedIndexNames) {
        final Set<String> results = new HashSet<String>();
        if (commaSeparatedIndexNames != null) {
            for (String name : Splitter.on(',').omitEmptyStrings().trimResults().split(commaSeparatedIndexNames)) {
                results.add(name);
            }
        }
        return results;
    }
}
