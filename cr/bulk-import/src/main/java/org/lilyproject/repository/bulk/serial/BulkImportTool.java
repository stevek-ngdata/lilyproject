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

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.bulk.BulkIngester;
import org.lilyproject.repository.bulk.LineMapper;
import org.lilyproject.repository.bulk.LineMappingContext;
import org.lilyproject.repository.bulk.jython.JythonLineMapper;
import org.python.google.common.io.Files;

/**
 * A bulk import tool similar to {@link org.lilyproject.repository.bulk.mapreduce.BulkImportTool} that works without
 * MapReduce.
 */
public class BulkImportTool {
    
    private static final Log LOG = LogFactory.getLog(BulkImportTool.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: BulkImportTool <lily zookeeper> <inputfile> <python file> <python symbol>");
            System.exit(1);
        }

        String lilyZk = args[0];
        String inputPath = args[1];
        String pythonFilePath = args[2];
        String pythonSymbol = args[3];

        BulkIngester bulkIngester = BulkIngester.newBulkIngester(lilyZk, 30000);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputPath));
        
        LineMapper lineMapper = new JythonLineMapper(Files.toString(new File(pythonFilePath), Charsets.UTF_8),
                pythonSymbol);
        ThreadedRecordWriter recordWriter = new ThreadedRecordWriter(lilyZk, 10);
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
        LOG.info(String.format("Imported %d lines in %.2f seconds", numLines, duration));
    }

}
