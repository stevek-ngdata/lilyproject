/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.bulk.serial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Charsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.bulk.AbstractBulkImportCliTool;
import org.lilyproject.repository.bulk.BulkIngester;
import org.lilyproject.repository.bulk.LineMapper;
import org.lilyproject.repository.bulk.LineMappingContext;
import org.lilyproject.repository.bulk.RecordWriter;
import org.lilyproject.repository.bulk.jython.JythonLineMapper;
import org.python.google.common.io.Files;

/**
 * A bulk import tool similar to {@link org.lilyproject.repository.bulk.mapreduce.BulkImportTool} that works without
 * MapReduce.
 */
public class BulkImportTool extends AbstractBulkImportCliTool {
    
    private final Log log = LogFactory.getLog(BulkImportTool.class);

    private Option dryRunOption;
    
    private boolean dryRun;
    
    @Override
    public List<Option> getOptions() {
        dryRunOption = OptionBuilder
                            .withDescription("Only print out the created records without writing them to Lily")
                            .withLongOpt("dryrun")
                            .create('d');
        
        List<Option> options = super.getOptions();
        options.add(dryRunOption);
        return options;
    }
    
    
    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int status = super.processOptions(cmd);
        if (status != 0) {
            return status;
        }
        
        dryRun = cmd.hasOption(dryRunOption.getOpt());
        return 0;
    }
   

    @Override
    protected String getCmdName() {
        return "lily-bulk-import";
    }
    
    @Override
    public int run(CommandLine cmd) throws Exception {
        BulkIngester bulkIngester = BulkIngester.newBulkIngester(zkConnectionString, 30000, outputTable);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputPath));
        
        LineMapper lineMapper = new JythonLineMapper(Files.toString(new File(pythonMapperPath), Charsets.UTF_8),
                pythonSymbol);
        RecordWriter recordWriter;
        if (dryRun) {
            recordWriter = new DebugRecordWriter(System.out);
        } else {
            recordWriter = new ThreadedRecordWriter(zkConnectionString, 10, outputTable);
        }
        LineMappingContext mappingContext = new LineMappingContext(bulkIngester, recordWriter);
        
        long start = System.currentTimeMillis();
        int numLines = 0;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            lineMapper.mapLine(line, mappingContext);
            numLines++;
        }
        
        bufferedReader.close();
        recordWriter.close();
        float duration = (System.currentTimeMillis() - start) / 1000f;
        log.info(String.format("Imported %d lines in %.2f seconds", numLines, duration));
        
        return 0;
    }

    public static void main(String[] args) throws IOException {
        new BulkImportTool().start(args);
    }
    
}
