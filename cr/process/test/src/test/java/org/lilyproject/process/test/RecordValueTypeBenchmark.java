/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.process.test;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.json.JsonUtil;

public class RecordValueTypeBenchmark {
    
    private MappingJsonFactory mappingJsonFactory = new MappingJsonFactory();
    
    private Repository repository;
    private TypeManager typeManager;

    private LilyProxy lilyProxy;

	private int nrOfFields = 20;
    private int nrOfRecords = 10000;
    private int nrOfTimes = 10;
    private enum Type {MIXED, SEQ, RVT, JSON};
    private Type type = Type.MIXED;
    
    // durations
    long rvtBuild = 0;
    long rvtCreate = 0;
    long rvtRead = 0;
    long rvtReadValues = 0;

    long jsonBuild = 0;
    long jsonCreate = 0;
    long jsonRead = 0;
    long jsonReadValues = 0;

	private List<RecordId> rvtRecordIds;

	private List<RecordId> jsonRecordIds;

	private String ns;


    public static void main(String[] args) throws Exception {
        RecordValueTypeBenchmark benchmark = new RecordValueTypeBenchmark();
        benchmark.initialize(args);
        
        benchmark.testRecordValueTypePerformance();
        
        benchmark.stop();
    }

    @Test
    public void testBenchmark() throws Exception {
        RecordValueTypeBenchmark benchmark = new RecordValueTypeBenchmark();
        benchmark.initialize(new String[] { "f=2" });

        benchmark.testRecordValueTypePerformance();

        benchmark.stop();
    }

    private void initialize(String[] args) throws Exception {
    	for (String arg : args) {
			if (arg.startsWith("f=")) {
				nrOfFields = Integer.valueOf(arg.substring(2));
			}
			if (arg.startsWith("r=")) {
                nrOfRecords = Integer.valueOf(arg.substring(2));
			}
			if (arg.startsWith("t=")) {
				type = Type.valueOf(arg.substring(2));
    		}
			if (arg.startsWith("n=")) {
				nrOfTimes = Integer.valueOf(arg.substring(2));
    		}
		}
        lilyProxy = new LilyProxy();
        lilyProxy.start();
        LilyClient lilyClient = lilyProxy.getLilyServerProxy().getClient();
        repository = lilyClient.getRepository();
        typeManager = repository.getTypeManager();
        resetDurations();
    }
    
    private void resetDurations() {
    	// durations
        rvtBuild = 0;
        rvtCreate = 0;
        rvtRead = 0;
        rvtReadValues = 0;

        jsonBuild = 0;
        jsonCreate = 0;
        jsonRead = 0;
        jsonReadValues = 0;
    }

    private void stop() throws Exception {
        lilyProxy.stop();
    }
    
    private void testRecordValueTypePerformance() throws Exception {
        System.out.println("===> Starting benchmark with settings: fields=" + nrOfFields + ", records=" + nrOfRecords
                + ", times=" + nrOfTimes + ", type=" + type.toString());
        
        ns = "testRecordValueTypePerformance";
        QName rvtRTName = new QName(ns, "rvtRT");
        
        // Create field types and record types for the records containing complex types
        RecordTypeBuilder rtBuilder = typeManager.recordTypeBuilder().name(rvtRTName);
        for (int i = 0; i < nrOfFields; i++) {
            FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager
                    .getValueType("STRING"), new QName(ns, "stringField" + i), Scope.NON_VERSIONED));
            rtBuilder.field(fieldType.getId(), false);
        }
        rtBuilder.create();
        
        ValueType rvt = typeManager.getValueType("RECORD<"+rvtRTName.toString()+">");
        
        FieldType rvtFT = typeManager.createFieldType(typeManager.newFieldType(rvt, new QName(ns, "rvtField"),
                Scope.NON_VERSIONED));
        typeManager.recordTypeBuilder().name(new QName(ns, "rvtFieldRT")).field(rvtFT.getId(), false).create();
        
        // Create field types and record types for the records containing serialized json
        FieldType jsonFT = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"),
                new QName(ns, "jsonField"), Scope.NON_VERSIONED));
        typeManager.recordTypeBuilder().name(new QName(ns, "jsonFieldRT")).field(jsonFT.getId(), false).create();

        rvtRecordIds = new ArrayList<RecordId>();
        jsonRecordIds = new ArrayList<RecordId>();
        
        switch (type) {
		case MIXED:
			for (int i = 0; i < nrOfTimes; i++) {
				resetDurations();
				runMixed();
			}
			break;
		case SEQ:
			for (int i = 0; i < nrOfTimes; i++) {
				resetDurations();
				runSequential();
			}
			break;
		case RVT:
			for (int i = 0; i < nrOfTimes; i++) {
				resetDurations();
				runRvt();
			}
			break;
		case JSON:
			for (int i = 0; i < nrOfTimes; i++) {
				resetDurations();
				runJson();
			}
			break;

		default:
			break;
		}
        System.out.println("===> End benchmark");
    }
    
    private void runMixed() throws Exception {
    	// Build and create the records
        for (int i = 0; i < nrOfRecords; i++) {
            long rvtBuildBefore = System.currentTimeMillis();
            Record rvtRecord = buildRvtRecord(ns, nrOfFields);
            rvtBuild += (System.currentTimeMillis() - rvtBuildBefore);

            long jsonBuildBefore = System.currentTimeMillis();
            Record jsonRecord = buildJsonRecord(ns, nrOfFields);
            jsonBuild += (System.currentTimeMillis() - jsonBuildBefore);

            long rvtCreateBefore = System.currentTimeMillis();
            rvtRecordIds.add(repository.create(rvtRecord).getId());
            rvtCreate += (System.currentTimeMillis() - rvtCreateBefore);
            
            long jsonCreateBefore = System.currentTimeMillis();
            jsonRecordIds.add(repository.create(jsonRecord).getId());
            jsonCreate += (System.currentTimeMillis() - jsonCreateBefore);
        }
        
        // Read the records and their values
        for (int i = 0; i < nrOfRecords; i++) {
            long rvtReadBefore = System.currentTimeMillis();
            Record rvtRecord = repository.read(rvtRecordIds.get(i));
            rvtRead += (System.currentTimeMillis() - rvtReadBefore);
            
            long jsonReadBefore = System.currentTimeMillis();
            Record jsonRecord = repository.read(jsonRecordIds.get(i));
            jsonRead += (System.currentTimeMillis() - jsonReadBefore);

            long rvtReadValuesBefore = System.currentTimeMillis();
            readRvtValues(rvtRecord, ns, nrOfFields);
            rvtReadValues += (System.currentTimeMillis() - rvtReadValuesBefore);
            
            long jsonReadValuesBefore = System.currentTimeMillis();
            readJsonValues(jsonRecord, ns, nrOfFields);
            jsonReadValues += (System.currentTimeMillis() - jsonReadValuesBefore);
        }
        long rvtTotal = rvtBuild + rvtCreate + rvtRead + rvtReadValues;
        long rvtAvg = rvtTotal / nrOfRecords;
        long jsonTotal = jsonBuild + jsonCreate + jsonRead + jsonReadValues;
        long jsonAvg = jsonTotal / nrOfRecords;
        
        System.out.println("===> RVT: total: " + rvtTotal + " avg: " + rvtAvg + " build: " + rvtBuild + " create: "
                + rvtCreate + " read: " + rvtRead + " readValues: " + rvtReadValues);
        System.out.println("===> JSON: total: " + jsonTotal + " avg: " + jsonAvg + " build: " + jsonBuild + " create: "
                + jsonCreate + " read: " + jsonRead + " readValues: " + jsonReadValues);
        
//      assertTrue("Complex types encoding is more than 10% slower than json encoding", rvtTotal < (jsonTotal + (jsonTotal / 10)));

    }

    private void runRvt() throws Exception {
    	// Build and create the records
        for (int i = 0; i < nrOfRecords; i++) {
            long rvtBuildBefore = System.currentTimeMillis();
            Record rvtRecord = buildRvtRecord(ns, nrOfFields);
            rvtBuild += (System.currentTimeMillis() - rvtBuildBefore);

            long rvtCreateBefore = System.currentTimeMillis();
            rvtRecordIds.add(repository.create(rvtRecord).getId());
            rvtCreate += (System.currentTimeMillis() - rvtCreateBefore);
            
        }
        
        // Read the records and their values
        for (int i = 0; i < nrOfRecords; i++) {
            long rvtReadBefore = System.currentTimeMillis();
            Record rvtRecord = repository.read(rvtRecordIds.get(i));
            rvtRead += (System.currentTimeMillis() - rvtReadBefore);
            
            long rvtReadValuesBefore = System.currentTimeMillis();
            readRvtValues(rvtRecord, ns, nrOfFields);
            rvtReadValues += (System.currentTimeMillis() - rvtReadValuesBefore);
            
        }
        long rvtTotal = rvtBuild + rvtCreate + rvtRead + rvtReadValues;
        long rvtAvg = rvtTotal / nrOfRecords;
        
        System.out.println("RVT: total: " + rvtTotal + " avg: " + rvtAvg + " build: " + rvtBuild + " create: " + rvtCreate + " read: " + rvtRead + " readValues: " + rvtReadValues);
    }

    private void runJson() throws Exception {
    	// Build and create the records
        for (int i = 0; i < nrOfRecords; i++) {
            long jsonBuildBefore = System.currentTimeMillis();
            Record jsonRecord = buildJsonRecord(ns, nrOfFields);
            jsonBuild += (System.currentTimeMillis() - jsonBuildBefore);

            long jsonCreateBefore = System.currentTimeMillis();
            jsonRecordIds.add(repository.create(jsonRecord).getId());
            jsonCreate += (System.currentTimeMillis() - jsonCreateBefore);
        }
        
        // Read the records and their values
        for (int i = 0; i < nrOfRecords; i++) {
            long jsonReadBefore = System.currentTimeMillis();
            Record jsonRecord = repository.read(jsonRecordIds.get(i));
            jsonRead += (System.currentTimeMillis() - jsonReadBefore);

            long jsonReadValuesBefore = System.currentTimeMillis();
            readJsonValues(jsonRecord, ns, nrOfFields);
            jsonReadValues += (System.currentTimeMillis() - jsonReadValuesBefore);
        }
        long jsonTotal = jsonBuild + jsonCreate + jsonRead + jsonReadValues;
        long jsonAvg = jsonTotal / nrOfRecords;
        
        System.out.println("JSON: total: " + jsonTotal + " avg: " + jsonAvg + " build: " + jsonBuild + " create: " + jsonCreate + " read: " + jsonRead + " readValues: " + jsonReadValues);
    }
    
    private void runSequential() throws Exception {
    	runJson();
    	runRvt();
    }
    
    private Record buildRvtRecord(String ns, int nrOfFields) throws Exception {
        RecordBuilder rvtRecordBuilder = repository.recordBuilder().defaultNamespace(ns);
        for (int i = 0; i < nrOfFields; i++) {
            rvtRecordBuilder.field("stringField"+i, "value"+i);
        }
        return repository.recordBuilder().defaultNamespace(ns).recordType("rvtFieldRT").field("rvtField", rvtRecordBuilder.build()).build();
    }
    
    private Record buildJsonRecord(String ns, int nrOfFields) throws Exception {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        for (int i = 0; i < nrOfFields; i++) {
            node.put("stringField"+i, "value"+i);
        }
        return repository.recordBuilder().defaultNamespace(ns).recordType("jsonFieldRT").field("jsonField", node.toString()).build();
    }
    
    private void readRvtValues(Record record, String ns, int nrOfFields) {
        Record rvtRecord = record.getField(new QName(ns, "rvtField"));
        for (int i = 0; i < nrOfFields; i++) {
            rvtRecord.getField(new QName(ns, "stringField"+i));
        }
    }
    
    private void readJsonValues(Record record, String ns, int nrOfFields) throws Exception {
        String jsonString = record.getField(new QName(ns, "jsonField"));
        JsonNode jsonNode = mappingJsonFactory.createJsonParser(jsonString).readValueAsTree();
        for (int i = 0; i < nrOfFields; i++) {
            JsonUtil.getString(jsonNode, "stringField"+i);
        }
    }

}
