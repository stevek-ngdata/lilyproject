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
package org.lilyproject.repository.bulk;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.util.Version;

/**
 * Extension point that provides argument parsing for bulk importing.
 */
public abstract class AbstractBulkImportCliTool extends BaseZkCliTool {
    
    private Option inputPathArg;
    private Option pythonMapperPathArg;
    private Option pythonSymbolArg;
    private Option outputTableArg;

    /** Path to the Python mapping script. */
    protected String pythonMapperPath;
    
    /** Python symbol to be used for mapping. */
    protected String pythonSymbol;
    
    /** Input path for the bulk import process. */
    protected String inputPath;
    
    /** Repository table where output is to be written. */
    protected String outputTable;
    
    public AbstractBulkImportCliTool() {
        
        inputPathArg = OptionBuilder
                .withDescription("Input path")
                .withLongOpt("input")
                .hasArg()
                .create('i');

        pythonMapperPathArg = OptionBuilder
                .withDescription("Path to Python mapper file")
                .withLongOpt("pyfile")
                .hasArg()
                .create('p');

        pythonSymbolArg = OptionBuilder
                .withDescription("Python mapper symbol")
                .withLongOpt("symbol")
                .hasArg()
                .create('s');
        
        outputTableArg = OptionBuilder
                .withDescription("Repository table name (defaults to record)")
                .withLongOpt("table")
                .hasArg()
                .create('t');

    }
    


    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", getCmdName());
    }
    
    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        options.add(inputPathArg);
        options.add(pythonMapperPathArg);
        options.add(pythonSymbolArg);
        options.add(outputTableArg);
        return options;
    }
    
    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int status = super.processOptions(cmd);
        if (status != 0) {
            return status;
        }
        
        if (!cmd.hasOption(inputPathArg.getOpt())) {
            System.err.println("No input path supplied");
            return 1;
        } else {
            inputPath = cmd.getOptionValue(inputPathArg.getOpt());
        }

        if (!cmd.hasOption(pythonMapperPathArg.getOpt())) {
            System.err.println("No python mapper file supplied");
            return 1;
        } else {
            pythonMapperPath = cmd.getOptionValue(pythonMapperPathArg.getOpt());
        }

        if (!cmd.hasOption(pythonSymbolArg.getOpt())) {
            System.err.println("No mapper symbol supplied");
            return 1;
        } else {
            pythonSymbol = cmd.getOptionValue(pythonSymbolArg.getOpt());
        }
        
        if (cmd.hasOption(outputTableArg.getOpt())) {
            outputTable = cmd.getOptionValue(outputTableArg.getOpt());
        }
        
        return 0;
    }
    
}
