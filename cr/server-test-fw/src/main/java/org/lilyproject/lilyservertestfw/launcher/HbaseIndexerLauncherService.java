package org.lilyproject.lilyservertestfw.launcher;

import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.indexer.hbase.mapper.LilyIndexerLifecycleEventListener;

import java.io.File;
import java.util.List;

public class HbaseIndexerLauncherService implements LauncherService{
    private Main hbaseIndexerService;
    private Configuration conf;
    @Override
    public void addOptions(List<Option> options) {
    }

    @Override
    public int setup(CommandLine cmd, File testHome, boolean clearData) throws Exception {
        hbaseIndexerService = new Main();
        conf = HBaseIndexerConfiguration.create();
        conf.set("hbaseindexer.lifecycle.listeners", LilyIndexerLifecycleEventListener.class.toString());
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int start(List<String> postStartupInfo) throws Exception {
        hbaseIndexerService.startServices(conf);
        return 0;
    }

    @Override
    public void stop() {
        hbaseIndexerService.stopServices();
    }
}
