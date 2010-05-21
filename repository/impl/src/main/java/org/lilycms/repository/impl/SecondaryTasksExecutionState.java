package org.lilycms.repository.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.wal.api.WalEntryId;

public class SecondaryTasksExecutionState {

	private Map<String, String> taskStates = new HashMap<String, String>();
	private WalEntryId walEntryId;
	
	public SecondaryTasksExecutionState(WalEntryId walEntryId) {
		this.walEntryId = walEntryId;
    }
	
	public void setTaskState(String task, String state) {
		taskStates.put(task, state);
	}
	
	public WalEntryId getWalEntryId() {
		return walEntryId;
	}
	
	public byte[] toBytes() {
		JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();

        object.put("walEntryId", walEntryId.toBytes());
        ArrayNode taskStatesNode = object.putArray("taskStates");
        for (Entry<String, String> entry : taskStates.entrySet()) {
        	ObjectNode taskStateNode = factory.objectNode();
        	taskStateNode.put("id", entry.getKey());
        	taskStateNode.put("state", entry.getValue());
	        taskStatesNode.add(taskStateNode);
        }
        
        return toJsonBytes(object);
	}
	
	
	public byte[] toJsonBytes(JsonNode jsonNode) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(os, jsonNode);
        } catch (IOException e) {
            // Small chance of this happening, since we are writing to a byte array
            throw new RuntimeException(e);
        }
        return os.toByteArray();
    }
	
	public static SecondaryTasksExecutionState fromBytes(byte[] bytes) throws IOException {
		
		JsonNode node = new ObjectMapper().readValue(bytes, 0, bytes.length, JsonNode.class);
		SecondaryTasksExecutionState stes = new SecondaryTasksExecutionState(WalEntryId.fromBytes(node.get("walEntryId").getBinaryValue()));
		
		 JsonNode taskStatesNode = node.get("taskStates");
		 for (int i = 0; i < taskStatesNode.size(); i++) {
			 JsonNode taskStateNode = taskStatesNode.get(i);
			 String task = taskStateNode.get("id").getTextValue();
			 String state = taskStateNode.get("state").getTextValue();
			 stes.setTaskState(task, state);
         }
		 
		return stes;
	}

	public String getTaskState(String taskId) {
		return taskStates.get(taskId);
    }
}
