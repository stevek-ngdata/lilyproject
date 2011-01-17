package org.lilyproject.clientmetrics.postproc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.lilyproject.cli.BaseCliTool;

import java.io.*;
import java.text.Collator;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Makes a nice report with graphs based on a plain metrics output file.
 */
public class MetricsReportTool extends BaseCliTool {
    private Option metricsFileOption;

    private Option outputDirOption;

    private static final char SEP = ' ';

    private static final String STRING_QUOTE = "\"";

    private static final int COLS_PER_METRIC = 5;

    private NumberFormat doubleFormat = new DecimalFormat("0.00");

    private DateTimeFormatter timeFormat = DateTimeFormat.forPattern("yyyyMMDDkkmmss");

    @Override
    protected String getCmdName() {
        return "lily-metrics-report";
    }

    public static void main(String[] args) {
        new MetricsReportTool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        metricsFileOption = OptionBuilder
                .isRequired()
                .withArgName("filename")
                .hasArg()
                .withDescription("Name of the input metrics file")
                .withLongOpt("metrics-file")
                .create("m");
        options.add(metricsFileOption);

        outputDirOption = OptionBuilder
                .isRequired()
                .withArgName("dirname")
                .hasArg()
                .withDescription("Name of the output dir (non-existing)")
                .withLongOpt("output-dir")
                .create("o");
        options.add(outputDirOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        File metricFile = new File(cmd.getOptionValue(metricsFileOption.getOpt()));
        if (!metricFile.exists()) {
            System.err.println("Specified metrics file does not exist: " + metricFile.getAbsolutePath());
            return 1;
        }

        File outputDir = new File(cmd.getOptionValue(outputDirOption.getOpt()));
        if (outputDir.exists()) {
            System.err.println("Specified output directory already exists: " + outputDir.getAbsolutePath());
            return 1;
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

        // Include a copy of the original input metrics file in the output dir
        System.out.println("Copy original metrics file in output directory");
        FileUtils.copyFile(metricFile, new File(outputDir, metricFile.getName()));

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

        for (Map.Entry<GroupName, List<String>> entry : groups.entrySet()) {
            writeDataFile(entry.getKey(), entry.getValue(), test, outputDir);
        }
        System.out.println();

        for (Map.Entry<GroupName, List<String>> entry : groups.entrySet()) {
            writePlotScript(entry.getKey(), entry.getValue(), outputDir);
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

        StringBuffer titleLine = new StringBuffer();
        titleLine.append(STRING_QUOTE).append("title").append(STRING_QUOTE);

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

        for (Interval interval : test.intervals) {
            ps.print(timeFormat.print(interval.begin));

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

    private void writePlotScript(GroupName groupName, List<String> metricNames, File outputDir) throws IOException {
        File file = new File(outputDir, groupName.fileName + ".plot.txt");
        System.out.println("Writing plot script " + file);
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

        ps.println("set terminal pngcairo enhanced rounded linewidth 1 size 1300, 500");
        ps.println("set output \"" + groupName.fileName + ".png\"");
        ps.println("set autoscale");
        ps.println("set title '" + groupName.title + "'");
        ps.println("set xdata time");
        ps.println("set timefmt \"%Y%m%d%H%M%S\"");
        ps.println("set key autotitle columnheader");
        ps.println("set datafile missing 'NaN'");
        ps.println("set ylabel \"unit depends on metric, times usually in ms\"");
        ps.println("set xlabel \"time\"");
        ps.println("set grid");

        final int firstPlotValue = 2;
        int lastPlotValue = 4;
        if (groupName.name.startsWith("-")) {
            // if the name starts with a dash, it means the values for avg/med/min/max are (intended to be) the same
            lastPlotValue = 2;
        }

        StringBuffer plot = new StringBuffer();
        plot.append("plot ");
        for (int i = 0; i < metricNames.size(); i++) {
            for (int c = firstPlotValue; c <= lastPlotValue; c++) {
                if (i > 0 || c > firstPlotValue)
                    plot.append(", ");

                int dataCol = (COLS_PER_METRIC * i) + 1 + c;
                plot.append("'").append(groupName.fileName).append(".txt").append("' using 1:").append(dataCol).append(" with steps");
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
            ps.println("<img src='" + group.fileName  + ".png'/><br/>");
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
            if (groupName.startsWith("-"))
                groupName = groupName.substring(1);
            return groupName;
        }

        private String groupNameToTitle(String groupName) {
            if (groupName.startsWith("-")) {
                return groupName.substring(1);
            } else {
                return groupName;
            }
        }

        public int compareTo(GroupName o) {
            return collator.compare(title, o.title);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            GroupName other = (GroupName) obj;
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
            List<String> list = super.get(key);
            if (list == null) {
                list = new ArrayList<String>();
                put(groupName, list);
            }
            return list;
        }
    }
}
