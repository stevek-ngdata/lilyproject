/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.tools.generatesplitkeys;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.cli.OptionUtil;
import org.lilyproject.util.Version;

import java.util.Arrays;
import java.util.List;

/**
 * A tool to generate split keys for use in conf/general/tables.xml.
 */
public class GenerateSplitKeys extends BaseCliTool {
    private Option uuidSplitsOption;
    private Option uuidSplitsLengthOption;
    private Option userIdSplitsOption;
    private Option userIdSplitsLengthOption;
    private Option noPrefixOption;

    public static void main(String[] args) {
        new GenerateSplitKeys().start(args);
    }

    @Override
    protected String getCmdName() {
        return "lily-generate-split-keys";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-generate-split-keys");
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        uuidSplitsOption = OptionBuilder
                .withArgName("count")
                .hasArg()
                .withDescription("Generate this amount of UUID splits")
                .withLongOpt("uuid-splits")
                .create("us");
        options.add(uuidSplitsOption);

        uuidSplitsLengthOption = OptionBuilder
                .withArgName("length")
                .hasArg()
                .withDescription("Length of UUID split key in bytes")
                .withLongOpt("uuid-splits-length")
                .create("usl");
        options.add(uuidSplitsLengthOption);

        userIdSplitsOption = OptionBuilder
                .withArgName("count")
                .hasArg()
                .withDescription("Generate this amount of USER ID splits")
                .withLongOpt("userid-splits")
                .create("is");
        options.add(userIdSplitsOption);

        userIdSplitsLengthOption = OptionBuilder
                .withArgName("length")
                .hasArg()
                .withDescription("Length of USER split key in bytes")
                .withLongOpt("userid-splits-length")
                .create("isl");
        options.add(userIdSplitsLengthOption);

        noPrefixOption = OptionBuilder
                .withDescription("Do not include the type-prefix byte")
                .withLongOpt("no-prefix")
                .create("np");
        options.add(noPrefixOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        int uuidSplits = OptionUtil.getIntOption(cmd, uuidSplitsOption, -1);
        int uuidSplitsLength = OptionUtil.getIntOption(cmd, uuidSplitsLengthOption, 3);
        int userIdSplits = OptionUtil.getIntOption(cmd, userIdSplitsOption, -1);
        int userIdSplitsLength = OptionUtil.getIntOption(cmd, userIdSplitsLengthOption, 3);

        boolean noPrefix = cmd.hasOption(noPrefixOption.getOpt());

        if (uuidSplits != -1 && userIdSplits != -1) {
            String splitKeys = generateUserHexadecimalSplits(userIdSplits, userIdSplitsLength, noPrefix) +
                    "," + Bytes.toStringBinary(new byte[] { 1 }) + "," +
                    generateUuidSplits(uuidSplits, uuidSplitsLength, noPrefix);
            System.out.println(splitKeys);
        } else if (uuidSplits != -1) {
            System.out.println(generateUuidSplits(uuidSplits, uuidSplitsLength, noPrefix));
        } else if (userIdSplits != -1) {
            System.out.println(generateUserHexadecimalSplits(userIdSplits, userIdSplitsLength, noPrefix));
        } else {
            System.out.println("Nothing to do, use -h to get help.");
        }

        return 0;
    }

    public String generateUuidSplits(int regionCount, int splitKeyLength, boolean noPrefix) {
        byte[] startBytes = new byte[] {};
        byte[] endBytes =  new byte[splitKeyLength];
        for (int i = 0; i < endBytes.length; i++) {
            endBytes[i] = (byte)0xFF;
        }

        // number of splits = number of regions - 1
        byte[][] splitKeys = Bytes.split(startBytes, endBytes, regionCount - 1);
        // Stripping the first key to avoid a region [null,0[ which will always be empty
        // And the last key to avoid [xffxffxff....,null[ to contain only few values if variants are created
        // for a record with record id xffxffxff.....
        splitKeys = Arrays.copyOfRange(splitKeys, 1, splitKeys.length - 1);

        StringBuilder builder = new StringBuilder();
        for (byte[] splitKey : splitKeys) {
            if (builder.length() > 0)
                builder.append(",");

            byte[] fullSplitKey;
            if (noPrefix) {
                fullSplitKey = splitKey;
            } else {
                fullSplitKey = new byte[splitKey.length + 1];
                fullSplitKey[0] = 1; // UUID record id's start with a 1 byte
                System.arraycopy(splitKey, 0, fullSplitKey, 1, splitKey.length);
            }

            builder.append(Bytes.toStringBinary(fullSplitKey));
        }

        return builder.toString();
    }

    /**
     * Calculates split keys for USER ID's assuming they contain random hexadecimal data,
     * typical use-case or keys prefixed with a hash (in hexadecimal notation).
     */
    public String generateUserHexadecimalSplits(int regionCount, int splitKeyLength, boolean noPrefix) {
        // Since it's hexadecimal, every character can take 16 values, and if we have e.g. 3 of them we
        // have 16*16*16 = 16^3 possible values
        double space = Math.pow(16, splitKeyLength);
        // Partition these values over the number of regions requested
        double part = space / (double)regionCount;

        StringBuilder builder = new StringBuilder();
        double current = 0;
        for (int i = 0; i < regionCount - 1; i++) {
            if (builder.length() > 0)
                builder.append(",");
            current += part;

            if (!noPrefix) {
                builder.append(Bytes.toStringBinary(new byte[] { 0 }));
            }
            builder.append(Long.toHexString(Math.round(current)));
        }

        return builder.toString();
    }
}
