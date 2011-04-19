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

import java.util.Iterator;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.springframework.beans.factory.annotation.Autowired;

@Path("rowlog")
public class RowLogResource {
	
	@Autowired
    protected RowLogConfigurationManager rowLogConfMgr;
	
    @GET
    @Produces("application/json")
    public ObjectNode get(@Context UriInfo uriInfo) throws Exception {
    	ObjectMapper m = new ObjectMapper();
    	ObjectNode result = m.convertValue(rowLogConfMgr.getRowLogs(), ObjectNode.class);
    	
    	Iterator<String> it = result.getFieldNames();
    	while (it.hasNext()) {
    		String rowLogId = it.next();
    		addSubscriptions((ObjectNode)result.get(rowLogId), rowLogId, m);
    	}
		return result;
    }

    @GET
    @Path("{id}")
    @Produces("application/json")
    public ObjectNode get(@PathParam("id") String rowLogId) throws Exception {
    	ObjectMapper m = new ObjectMapper();
    	RowLogConfig rowLogConfig = rowLogConfMgr.getRowLogs().get(rowLogId);
		ObjectNode result = m.convertValue(rowLogConfig, ObjectNode.class);
		
		addSubscriptions(result, rowLogId, m);
    	
		return result;
    }

	private void addSubscriptions(ObjectNode rowLogConfigNode, String rowLogId, ObjectMapper m)
			throws KeeperException, InterruptedException {
		List<RowLogSubscription> subscriptions = rowLogConfMgr.getSubscriptions(rowLogId);
    	
		ArrayNode subscriptionsJson = m.convertValue(subscriptions, ArrayNode.class);
		rowLogConfigNode.put("subscriptions", subscriptionsJson);
		
		Iterator<JsonNode> it = subscriptionsJson.iterator();
		while (it.hasNext()) {
			ObjectNode subJson = (ObjectNode)it.next();
			List<String> listeners = rowLogConfMgr.getListeners(rowLogId, subJson.get("id").getTextValue());
			subJson.put("listeners", m.convertValue(listeners, ArrayNode.class));
		}
	}

}