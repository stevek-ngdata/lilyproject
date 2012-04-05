package org.lilyproject.rest.perftest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.testclientfw.BaseTestTool;
import org.lilyproject.testclientfw.Util;
import org.lilyproject.testclientfw.Words;
import org.lilyproject.util.Version;
import org.lilyproject.util.json.JsonFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RestPerfTest extends BaseTestTool {

    private HttpClient httpclient;

    private String baseUri;

    private int iterations;

    private Option iterationsOption;

    public static void main(String[] args) throws Exception {
        new RestPerfTest().start(args);
    }

    @Override
    protected String getCmdName() {
        return "rest-perftest";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-rest-perftest");
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        iterationsOption = OptionBuilder
                .withArgName("iterations")
                .hasArg()
                .withDescription("Number of iterations")
                .withLongOpt("iterations")
                .create("i");
        options.add(iterationsOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        iterations = Util.getIntOption(cmd, iterationsOption, 10000);

        setupMetrics();

        ThreadSafeClientConnManager connMgr = new ThreadSafeClientConnManager();
        httpclient = new DefaultHttpClient(connMgr);

        baseUri = "http://localhost:12060/repository";

//        Logger log = Logger.getLogger("org.apache.http");
//        log.setLevel(Level.DEBUG);

        runTest();

        finishMetrics();

        return 0;
    }

    private void runTest() throws Exception {

        // Create 5 field types
        for (int i = 1; i <= 5; i++) {
            String body = json("{name: 'n$field" + i + "', valueType: 'STRING', " +
                    "scope: 'versioned', namespaces: { 'org.lilyproject.rest-perftest': 'n' } }");
            put("/schema/fieldType/n$field" + i + "?ns.n=org.lilyproject.rest-perftest",
                    body.getBytes(), 200, 201);
        }

        // Create a record type
        {
            String body = json("{name: 'n$recordType', fields: [ {name: 'n$field1'}, {name: 'n$field2'}, " +
                    "{name: 'n$field3'}, {name: 'n$field4'}, {name: 'n$field5'} ]," +
                    "namespaces: { 'org.lilyproject.rest-perftest': 'n' } }");
            put("/schema/recordType/n$recordType?ns.n=org.lilyproject.rest-perftest", body.getBytes(), 200, 201);
        }

        final List<String> recordIds = new ArrayList<String>();

        //
        // Create records using put
        //
        startExecutor();
        final JsonNodeFactory factory = JsonNodeFactory.instance;
        final long now = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ObjectNode recordNode = factory.objectNode();
                        recordNode.put("type", "n$recordType");

                        ObjectNode fieldsNode = recordNode.putObject("fields");
                        for (int f = 1; f <= 5; f++) {
                            fieldsNode.put("n$field" + f, Words.get());
                        }

                        ObjectNode namespaces = recordNode.putObject("namespaces");
                        namespaces.put("org.lilyproject.rest-perftest", "n");

                        byte[] recordBytes = JsonFormat.serializeAsBytes(recordNode);

                        String recordId = "USER." + now + "_" + seqnr;
                        recordIds.add(recordId);

                        long before = System.nanoTime();
                        put("/record/" + recordId, recordBytes, 201);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Create record using put", "C", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Create records using post
        //
        startExecutor();
        for (int i = 0; i < iterations; i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ObjectNode actionNode = factory.objectNode();
                        actionNode.put("action", "create");

                        ObjectNode recordNode = actionNode.putObject("record");
                        recordNode.put("type", "n$recordType");

                        ObjectNode fieldsNode = recordNode.putObject("fields");
                        for (int f = 1; f <= 5; f++) {
                            fieldsNode.put("n$field" + f, Words.get());
                        }

                        ObjectNode namespaces = recordNode.putObject("namespaces");
                        namespaces.put("org.lilyproject.rest-perftest", "n");

                        byte[] recordBytes = JsonFormat.serializeAsBytes(actionNode);

                        long before = System.nanoTime();
                        Result result = post("/record", recordBytes, 201);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Create record using post", "E", duration / 1e6d);

                        // ID is assigned by the server, read it out
                        ObjectNode createdRecord = (ObjectNode)JsonFormat.deserialize(result.data);
                        recordIds.add(createdRecord.get("id").getValueAsText());
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read records
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr), 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Record read", "R", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read records using vtag
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr) + "/vtag/last", 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Record read via last vtag", "T", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read records using version
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr) + "/version/1", 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Record read specific version", "V", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read list of versions
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr) + "/version", 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Read list of versions", "L", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read list of variants
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr) + "/variant", 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Read list of variants", "M", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Update records using PUT
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ObjectNode recordNode = factory.objectNode();

                        ObjectNode fieldsNode = recordNode.putObject("fields");
                        for (int f = 1; f <= 5; f++) {
                            fieldsNode.put("n$field" + f, Words.get());
                        }

                        ObjectNode namespaces = recordNode.putObject("namespaces");
                        namespaces.put("org.lilyproject.rest-perftest", "n");

                        byte[] recordBytes = JsonFormat.serializeAsBytes(recordNode);

                        String recordId = recordIds.get(seqnr);

                        long before = System.nanoTime();
                        put("/record/" + recordId, recordBytes, 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Update record using put", "U", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();

        //
        // Read records using version
        //
        startExecutor();
        for (int i = 0; i < recordIds.size(); i++) {
            final int seqnr = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long before = System.nanoTime();
                        get("/record/" + recordIds.get(seqnr) + "/version/2", 200);
                        double duration = System.nanoTime() - before;
                        metrics.increment("Record read specific version (2)", "W", duration / 1e6d);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        stopExecutor();
    }

    private Result get(String path, int... expectedStatus) throws IOException {
        HttpGet get = new HttpGet(baseUri + path);
        HttpResponse response = httpclient.execute(get);

        byte[] data = checkStatusAndReadResponse(path, response, expectedStatus);

        return new Result(response, data);
    }

    private Result put(String path, byte[] body, int... expectedStatus) throws IOException {
        HttpPut put = new HttpPut(baseUri + path);
        put.addHeader("Content-Type", "application/json");
        put.setEntity(new ByteArrayEntity(body));
        HttpResponse response = httpclient.execute(put);

        byte[] data = checkStatusAndReadResponse(path, response, expectedStatus);

        return new Result(response, data);
    }

    private Result post(String path, byte[] body, int... expectedStatus) throws IOException {
        HttpPost post = new HttpPost(baseUri + path);
        post.addHeader("Content-Type", "application/json");
        post.setEntity(new ByteArrayEntity(body));
        HttpResponse response = httpclient.execute(post);

        byte[] data = checkStatusAndReadResponse(path, response, expectedStatus);

        return new Result(response, data);
    }

    private byte[] checkStatusAndReadResponse(String path, HttpResponse response, int... expectedStatus)
            throws IOException {

        // It's important to read out the entity
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        response.getEntity().writeTo(bos);

        int status = response.getStatusLine().getStatusCode();

        Arrays.sort(expectedStatus);
        if (Arrays.binarySearch(expectedStatus, status) < 0) {
            System.out.println(new String(bos.toByteArray()));

            throw new RuntimeException("Unexpected response status. Got " + status + ", expected (one of) " +
                    toString(expectedStatus) + ". Request path: " + path);
        }

        return bos.toByteArray();
    }

    private String json(String input) {
        return input.replaceAll("'", "\"");
    }

    private String toString(int[] numbers) {
        StringBuilder builder = new StringBuilder();
        for (int number : numbers) {
            if (builder.length() > 0)
                builder.append(", ");
            builder.append(number);
        }
        return builder.toString();
    }

    private static class Result {
        HttpResponse response;
        byte[] data;

        public Result(HttpResponse response, byte[] data) {
            this.response = response;
            this.data = data;
        }
    }
}

