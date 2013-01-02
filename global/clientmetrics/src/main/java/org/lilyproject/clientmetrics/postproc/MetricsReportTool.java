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
package org.lilyproject.clientmetrics.postproc;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.Collator;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.lilyproject.cli.BaseCliTool;
import org.lilyproject.util.ConsoleUtil;
import org.lilyproject.util.Version;

/**
 * Makes a nice report with graphs based on a plain metrics output file.
 */
public class MetricsReportTool extends BaseCliTool {
    private Option metricsFileOption;

    private Option outputDirOption;

    private Option forceOption;

    private static final char SEP = ' ';

    private static final String STRING_QUOTE = "\"";

    private static final int COLS_PER_METRIC = 5;

    private static final int HEADER_COLUMNS = 2;

    private static final int COL_CNT = 1;
    private static final int COL_AVG = 2;
    private static final int COL_MED = 3;
    private static final int COL_MIN = 4;
    private static final int COL_MAX = 5;

    // http://www.uni-hamburg.de/Wiss/FB/15/Sustainability/schneider/gnuplot/colors.htm
    private static final String[] COLORS = new String[]{
            "#D2691E", /* chocolate */
            "#DC143C", /* crimson */
            "#00008B", /* darkblue */
            "#006400", /* darkgreen */
            "#FF8C00", /* darkorange */
            "#FF1493", /* deeppink */
            "#FFD700", /* gold */
            "#808000", /* olive */
            "#FF0000", /* red */
            "#708090", /* slategray */
            "#00008B"  /* darkblue */
    };

    private NumberFormat doubleFormat = new DecimalFormat("0.00");

    private DateTimeFormatter timeFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    @Override
    protected String getCmdName() {
        return "lily-metrics-report";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-clientmetrics");
    }

    public static void main(String[] args) {
        new MetricsReportTool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        metricsFileOption = OptionBuilder
                .withArgName("filename")
                .hasArg()
                .withDescription("Name of the input metrics file")
                .withLongOpt("metrics-file")
                .create("m");
        options.add(metricsFileOption);

        outputDirOption = OptionBuilder
                .withArgName("dirname")
                .hasArg()
                .withDescription("Name of the output dir")
                .withLongOpt("output-dir")
                .create("o");
        options.add(outputDirOption);

        forceOption = OptionBuilder.withDescription("Force using the output directory even if it already exists")
                .withLongOpt("force").create("f");
        options.add(forceOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        String metricFileName = cmd.getOptionValue(metricsFileOption.getOpt());
        if (metricFileName == null) {
            System.out.println("Specify metrics file name with -" + metricsFileOption.getOpt());
            return 1;
        }

        File metricFile = new File(metricFileName);
        if (!metricFile.exists()) {
            System.err.println("Specified metrics file does not exist: " + metricFile.getAbsolutePath());
            return 1;
        }

        String outputDirName = cmd.getOptionValue(outputDirOption.getOpt());
        if (outputDirName == null) {
            System.out.println("Specify output directory with -" + outputDirOption.getOpt());
            return 1;
        }

        File outputDir = new File(outputDirName);
        if (outputDir.exists()) {
            if (!cmd.hasOption(forceOption.getOpt())) {
                System.err.println("Specified output directory already exists: " + outputDir.getAbsolutePath());
                boolean proceed = ConsoleUtil.promptYesNo("Continue anyway? [y/N]", false);
                if (!proceed) {
                    return 1;
                }
            }
        }

        MetricsParser parser = new MetricsParser();
        Tests tests;
        try {
            tests = parser.parse(metricFile);
        } catch (Exception e) {
            System.err.println("Error occurred reading metrics file, current line: " + parser.getCurrentLine());
            System.err.println();
            e.printStackTrace();
            return 1;
        }

        if (tests.entries.size() == 0) {
            System.err.println("No test data found in specified metrics file");
            return 1;
        }

        for (Test test : tests.entries) {
            File testDir = new File(outputDir, test.name);
            FileUtils.forceMkdir(testDir);

            writeMetrics(test, testDir);
        }

        writeTestsInfo(tests, outputDir);

        // Include a copy of the original input metrics file in the output dir, except if the output is
        // produced in the same directory as where the metrics file is located
        if (!outputDir.getAbsoluteFile().getCanonicalFile().equals(metricFile.getAbsoluteFile().getParentFile().getCanonicalFile())) {
            System.out.println("Copy original metrics file in output directory");
            FileUtils.copyFile(metricFile, new File(outputDir, metricFile.getName()));
        }

        return 0;
    }

    private GroupMap writeMetrics(Test test, File outputDir) throws IOException, InterruptedException {

        // Determine how the metrics will be grouped into files (plots)

        // This map contains as key a group name, and as value the list of metrics that fall into that group
        GroupMap groups = new GroupMap();

        for (String metricName : test.metricNames.keySet()) {
            int colonPos = metricName.indexOf(':');
            int atPos = metricName.indexOf('@');

            if (colonPos > 0) { // grouped by type
                String group = metricName.substring(0, colonPos);
                groups.getByString(group).add(metricName);
            } else if (atPos > 0) { // custom grouping when not using types (e.g. used by the system metrics)
                String group = metricName.substring(0, atPos);
                groups.getByString(group).add(metricName);
            } else {
                // all the rest: each in its own group
                groups.getByString(metricName).add(metricName);
            }
        }

        for (List<String> list : groups.values()) {
            Collections.sort(list);
        }

        for (Map.Entry<GroupName, List<String>> entry : groups.entrySet()) {
            writeDataFile(entry.getKey(), entry.getValue(), test, outputDir);
        }
        System.out.println();

        for (Map.Entry<GroupName, List<String>> entry : groups.entrySet()) {
            writePlotScript(entry.getKey(), entry.getValue(), test, outputDir);
        }
        System.out.println();

        for (Map.Entry<GroupName, List<String>> entry : groups.entrySet()) {
            executePlot(entry.getKey(), outputDir);
        }
        System.out.println();

        writeHtmlReport(groups.keySet(), test, outputDir);

        System.out.println();

        return groups;
    }

    private void writeDataFile(GroupName groupName, List<String> metricNames, Test test, File outputDir) throws IOException {
        File file = new File(outputDir, groupName.fileName + ".txt");
        System.out.println("Writing data file " + file);
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

        StringBuilder titleLine = new StringBuilder();
        titleLine.append(STRING_QUOTE).append("time").append(STRING_QUOTE);
        titleLine.append(SEP);
        titleLine.append(STRING_QUOTE).append("seq").append(STRING_QUOTE);

        for (String metricName : metricNames) {
            titleLine.append(SEP);
            titleLine.append(STRING_QUOTE).append(removeGroupingPrefix(metricName)).append(" count").append(STRING_QUOTE);
            titleLine.append(SEP);
            titleLine.append(STRING_QUOTE).append(removeGroupingPrefix(metricName)).append(" avg").append(STRING_QUOTE);
            titleLine.append(SEP);
            titleLine.append(STRING_QUOTE).append(removeGroupingPrefix(metricName)).append(" med").append(STRING_QUOTE);
            titleLine.append(SEP);
            titleLine.append(STRING_QUOTE).append(removeGroupingPrefix(metricName)).append(" min").append(STRING_QUOTE);
            titleLine.append(SEP);
            titleLine.append(STRING_QUOTE).append(removeGroupingPrefix(metricName)).append(" max").append(STRING_QUOTE);
        }

        ps.println(titleLine.toString());

        int i = 0;
        for (Interval interval : test.intervals) {
            ps.print(timeFormat.print(interval.begin));
            ps.print(SEP);
            ps.print(i++);

            for (String metricName : metricNames) {
                int index = test.getIndex(metricName);
                MetricData data = safeGet(interval, index);
                ps.print(SEP);
                ps.print(formatLong(data.count));
                ps.print(SEP);
                ps.print(formatDouble(data.average));
                ps.print(SEP);
                ps.print(formatDouble(data.median));
                ps.print(SEP);
                ps.print(formatDouble(data.min));
                ps.print(SEP);
                ps.print(formatDouble(data.max));
            }

            ps.println();
        }

        ps.close();
    }

    private String removeGroupingPrefix(String metricName) {
        int colonPos = metricName.indexOf(':');
        int atPos = metricName.indexOf('@');

        if (colonPos > 0) {
            return metricName.substring(colonPos + 1);
        } else if (atPos > 0) {
            return metricName.substring(atPos + 1);
        } else {
            return metricName;
        }
    }

    private String formatDouble(double value) {
        if (value < 0) {
            return "NaN";
        } else {
            return doubleFormat.format(value);
        }
    }

    private String formatLong(long value) {
        if (value < 0) {
            return "NaN";
        } else {
            return String.valueOf(value);
        }
    }

    private MetricData safeGet(Interval interval, int index) {
        if (index >= interval.datas.length) {
            return new MetricData();
        } else {
            return interval.datas[index];
        }
    }

    private void writePlotScript(GroupName groupName, List<String> metricNames, Test test, File outputDir) throws IOException {
        File file = new File(outputDir, groupName.fileName + ".plot.txt");
        System.out.println("Writing plot script " + file);
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

        ps.println("set terminal pngcairo enhanced rounded linewidth 2 size 1300, 500");
        ps.println("set output \"" + groupName.fileName + ".png\"");
        ps.println("set autoscale");
        ps.println("set title '" + groupName.title + "'");
        ps.println("set key autotitle columnheader");
        ps.println("set datafile missing 'NaN'");
        ps.println("set ylabel \"unit depends on metric, times usually in ms\"");
        ps.println("set xlabel \"time\"");
        ps.println("set grid");

        // if the name starts with a dash, it means the values for avg/med/min/max are (intended to be) the same
        boolean isAvgOnly = groupName.name.startsWith("-");

        final int firstPlotValue = COL_AVG;
        int lastPlotValue = COL_MIN;
        if (isAvgOnly) {
            lastPlotValue = COL_AVG;
        }

        if (test.intervals.size() > 1) {
            // Calculate trendlines: on median except for avg-only metrics
            // The trendline is calculated against col 2, that is the column containing the seq numbers, since it
            // does not work with the date values (I think because they are too big integers?)
            for (int i = 0; i < metricNames.size(); i++) {
                ps.println("f" + i + "(x)=m" + i + "*x+c" + i);
                int dataCol = (COLS_PER_METRIC * i) + HEADER_COLUMNS + (isAvgOnly ? COL_AVG : COL_MED);
                ps.println("fit f" + i + "(x) \"" + groupName.fileName + ".txt\" using 2:" + dataCol + " via m" + i + ",c" + i);
            }
        }

        // Change xdata to time only after calculating trendlines
        ps.println("set xdata time");
        ps.println("set timefmt \"%Y%m%d%H%M%S\"");

        int numberOfValues = lastPlotValue - firstPlotValue + 1;

        StringBuilder plot = new StringBuilder();
        plot.append("plot ");
        for (int i = 0; i < metricNames.size(); i++) {
            int colorStart = i * numberOfValues;

            for (int c = firstPlotValue; c <= lastPlotValue; c++) {
                if (i > 0 || c > firstPlotValue) {
                    plot.append(", ");
                }

                int dataCol = (COLS_PER_METRIC * i) + HEADER_COLUMNS + c;
                int color = colorStart + c - firstPlotValue;
                plot.append("'").append(groupName.fileName).append(".txt' using 1:").append(dataCol).
                        append(" with steps linecolor rgb '").append(COLORS[color % COLORS.length]).append("'");
            }

            if (test.intervals.size() > 1) {
                // add trendline
                // same color as data line
                int color = colorStart + (isAvgOnly ? COL_AVG : COL_MED) - firstPlotValue;
                plot.append(", '").append(groupName.fileName).append(".txt' using 1:(f").append(i).append("($2))").
                        append(" with lines linewidth 1 linecolor rgb '").append(COLORS[color % COLORS.length]).append("' title '")
                        .append(removeGroupingPrefix(metricNames.get(i))).append(isAvgOnly ? " avg" : " med").append(" trend'");
            }
        }

        ps.println(plot.toString());

        ps.close();
    }

    private void executePlot(GroupName groupName, File outputDir) throws IOException, InterruptedException {
        System.out.println("Calling gnuplot for " + groupName);
        ProcessBuilder pb = new ProcessBuilder("gnuplot", groupName.fileName + ".plot.txt");
        pb.directory(outputDir);
        Process p = pb.start();
        int exitValue = p.waitFor();
        if (exitValue != 0) {
            System.err.println("Warning: gnuplot returned exit code: " + exitValue);
        }
    }

    private void writeHtmlReport(Set<GroupName> groupNames, Test test, File outputDir) throws IOException {
        File file = new File(outputDir, "report.html");
        System.out.println("Writing HTML report " + file);

        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

        ps.println("<html><body>");

        ps.println("<a href='../info.html'>General tests info</a>");

        ps.println("<h1>" + test.name + ": " + (test.description != null ? test.description : "(no title)") + "</h1>");

        List<GroupName> orderedGroupNames = new ArrayList<GroupName>(groupNames);
        Collections.sort(orderedGroupNames);

        for (GroupName group : orderedGroupNames) {
            ps.println("<img src='" + group.fileName + ".png'/><br/>");
        }

        ps.println("</body></html>");

        ps.close();
    }

    private void writeTestsInfo(Tests tests, File outputDir) throws IOException {
        File file = new File(outputDir, "info.html");
        System.out.println("Writing HTML report " + file);

        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

        ps.println("<html><body>");

        ps.println("<h1>Tests</h1>");

        ps.println("<ul>");
        for (Test test : tests.entries) {
            ps.println("<li><a href='" + test.name + "/report.html'>" + test.name + "</a>");
        }
        ps.println("</ul>");

        if (tests.header.size() > 0) {
            ps.println("<h1>Pre-tests information</h1>");
            ps.println("<p>System information before execution of the tests.");

            ps.print("<pre>");
            for (String line : tests.header) {
                ps.println(line);
            }
            ps.println("</pre>");
        }

        if (tests.footer.size() > 0) {
            ps.println("<h1>Post-tests information</h1>");

            ps.println("<p>System information after execution of the tests.");

            ps.print("<pre>");
            for (String line : tests.footer) {
                ps.println(line);
            }
            ps.println("</pre>");
        }

        ps.println("</body></html>");

        ps.close();
    }

    private static class GroupName implements Comparable<GroupName> {
        String name;
        String title;
        String fileName;
        private Collator collator = Collator.getInstance(Locale.US);

        public GroupName(String name) {
            this.name = name;
            this.title = groupNameToTitle(name);
            this.fileName = groupNameToFileName(name);
        }

        private String groupNameToFileName(String groupName) {
            groupName = groupName.replaceAll(Pattern.quote("/"), Matcher.quoteReplacement("_"));
            groupName = groupName.replaceAll(Pattern.quote(" "), Matcher.quoteReplacement("_"));
            if (groupName.startsWith("-")) {
                groupName = groupName.substring(1);
            }
            return groupName;
        }

        private String groupNameToTitle(String groupName) {
            if (groupName.startsWith("-")) {
                return groupName.substring(1);
            } else {
                return groupName;
            }
        }

        @Override
        public int compareTo(GroupName o) {
            return collator.compare(title, o.title);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GroupName other = (GroupName)obj;
            return name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class GroupMap extends HashMap<GroupName, List<String>> {
        public List<String> getByString(String key) {
            GroupName groupName = new GroupName(key);
            List<String> list = super.get(groupName);
            if (list == null) {
                list = new ArrayList<String>();
                put(groupName, list);
            }
            return list;
        }
    }
}
