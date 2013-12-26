package org.lilyproject.lilyservertestfw.launcher;

import java.io.File;
import java.util.List;

import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.indexer.hbase.mapper.LilyIndexerLifecycleEventListener;

public class HbaseIndexerLauncherService implements LauncherService {
    private Main hbaseIndexerService;
    private Configuration conf;

    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome, boolean clearData) throws Exception {
        hbaseIndexerService = new Main();
        conf = HBaseIndexerConfiguration.create();
        conf.set("hbaseindexer.lifecycle.listeners", LilyIndexerLifecycleEventListener.class.getName());
        return 0;
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        if (hbaseIndexerService == null) {
            setup(null, null, false);
        }

        hbaseIndexerService.startServices(conf);
        return 0;
    }

    @Override
    public void stop() {
        if (hbaseIndexerService != null)
            hbaseIndexerService.stopServices();
    }

    public Main getHbaseIndexerService() {
        return hbaseIndexerService;
    }
}
