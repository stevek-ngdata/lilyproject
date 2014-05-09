package org.lilyproject.tools;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.*;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.util.io.Closer;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.*;

public class KeepAliveCheck extends BaseZkCliTool {
    private final static String NAME = "lily-keepalive-check";
    private boolean keepAlive = false;
    private long timeout = 10000;
    private int numberOfAttempts = 3;

    private RecordId recordId;
    private LilyClient lilyClient;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private Option keepAliveOption;
    private Option timeoutOption;
    private Option numberOfAttemptsOption;


    public static void main(String[] args) throws Exception {
        new KeepAliveCheck().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        keepAliveOption = OptionBuilder
                .withDescription("Send keep alive signal")
                .withLongOpt("keep-alive")
                .create("k");

        timeoutOption = OptionBuilder
                .withArgName("timeout")
                .hasArg()
                .withDescription("Delay to check if connection is still alive")
                .withLongOpt("timeout")
                .create("t");

        numberOfAttemptsOption = OptionBuilder
                .withArgName("attempts")
                .hasArg()
                .withDescription("Number of times to poll the lily server. At least 2")
                .withLongOpt("attempts")
                .create("a");

        options.add(keepAliveOption);
        options.add(numberOfAttemptsOption);
        options.add(timeoutOption);

        return options;
    }

    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0) {
            return result;
        }

        keepAlive = cmd.hasOption(keepAliveOption.getOpt());
        if (cmd.hasOption(timeoutOption.getOpt())) {
            timeout = Long.parseLong(cmd.getOptionValue(timeoutOption.getOpt()));
            Preconditions.checkArgument(timeout >= 1, "The timeout must be at least 1ms./");
        }
        if (cmd.hasOption(numberOfAttemptsOption.getOpt())) {
            numberOfAttempts = Integer.parseInt(cmd.getOptionValue(numberOfAttemptsOption.getOpt()));
            Preconditions.checkArgument(numberOfAttempts >= 2, "The number of attempts must be at least 2");

        }

        return 0;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        init();
        final ScheduledFuture<?> checkHandle = executor.scheduleWithFixedDelay(new CheckTask(recordId, lilyClient), 5, timeout, TimeUnit.MILLISECONDS);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                checkHandle.cancel(false);
            }
        }, (timeout * (numberOfAttempts - 1)) + 100, TimeUnit.MILLISECONDS);

        while (!checkHandle.isDone()) {
            try {
                checkHandle.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                return 1;
            } catch (CancellationException e) {
                System.out.println("Done.");
            }
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        super.cleanup();
        if (lilyClient != null) {
            System.out.println("Deleting record : " + recordId);
            try {
                lilyClient.getDefaultRepository().getDefaultTable().delete(recordId);
            } catch (RepositoryException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Closer.close(lilyClient);
        }


        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void init() throws Exception{
        //
        // Instantiate Lily client
        //
        lilyClient = new LilyClient(this.zkConnectionString, 20000, keepAlive);
        lilyClient.getRetryConf().setRetryMaxTime(5000);

        LRepository repository = lilyClient.getDefaultRepository();
        LTable table = repository.getDefaultTable();

        //
        // Create a schema
        //
        // We do this using the import library: this is easier than writing
        // out the schema construction code in Java, and automatically handles
        // the case where the schema would already exist (on second run of this app).
        //
        System.out.println("Importing schema");
        InputStream is = KeepAliveCheck.class.getResourceAsStream("schema.json");
        JsonImport.loadSchema(repository, is);
        is.close();
        System.out.println("Schema successfully imported");

        //
        // Create a record
        //
        System.out.println("Creating a record");
        Record record = table.newRecord();
        record.setId(repository.getIdGenerator().newRecordId());
        record.setRecordType(q("Type1"));
        record.setField(q("field1"), "value1");
        // We use the createOrUpdate method as that one can automatically recover
        // from connection errors (idempotent behavior, like PUT in HTTP).
        table.createOrUpdate(record);
        System.out.println("Record created: " + record.getId());
        this.recordId = record.getId();
    }


    private static class CheckTask implements Runnable {
        private final LilyClient client;
        private final RecordId recordId;

        public CheckTask(RecordId recordId, LilyClient client) {
            this.client = client;
            this.recordId = recordId;
        }
        @Override
        public void run() {
            try {
                LRepository repository = client.getDefaultRepository();
                LTable table = repository.getDefaultTable();
                System.out.print("Polling ...  ");
                table.getVariants(recordId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (RepositoryException e) {
                System.out.println("Fail.");
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {

            }
            System.out.println("Success.");
        }
    }

    private static QName q(String name) {
        return new QName("org.lilyproject.keepalivecheck", name);
    }

    @Override
    protected String getCmdName() {
        return NAME;
    }

    @Override
    protected String getVersion() {
        return org.lilyproject.util.Version.readVersion("org.lilyproject", "lily-keepalive-check");
    }
}
