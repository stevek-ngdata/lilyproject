package org.lilyproject.clientmetrics.postproc;

import org.joda.time.DateTime;
import org.lilyproject.util.io.Closer;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Parses a metrics file as produced by {@Metrics}.
 */
public class MetricsParser {
    private static final String INT_START_LINE = "| Interval started at: ";

    private LineCountingReader reader;

    public Tests parse(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        if (file.getName().endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }

        try {
            return parse(is);
        } finally {
            Closer.close(is);
        }
    }

    public Tests parse(InputStream is) throws IOException {
        reader = new LineCountingReader(new InputStreamReader(is));

        Tests tests = new Tests();

        Test test = null;
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("=")) {
                // A metrics file can contain results from sequentially executed tests, we recognize the beginning
                // of each test at the = symbol. See Metrics.beginTest()
                String testDefinitionLine = reader.readLine();
                int colonPos = testDefinitionLine.indexOf(":");
                String testName = testDefinitionLine.substring(0, colonPos);
                test = new Test(testName);
                tests.entries.add(test);

                test.description = testDefinitionLine.substring(colonPos + 1).trim();

                // read test-heading closing line
                reader.readLine();
            } else if (line.startsWith("~~begin header")) {
                while ((line = reader.readLine()) != null && !line.equals("~~end header")) {
                    tests.header.add(line);
                }
            } else if (line.startsWith("~~begin footer")) {
                System.out.println("footer gevonden");
                while ((line = reader.readLine()) != null && !line.equals("~~end footer")) {
                    tests.footer.add(line);
                }
                System.out.println("footer gevonden:" + tests.footer.size());
            } else if (line.startsWith(INT_START_LINE)) {
                // The code below parses tables like the following one:
                //
                //+----------------------------------------------------------------------------------------------------------------------+
                //| Interval started at: 2011-01-15T19:57:23.918+01:00 (duration: 30s).                                                  |
                //| Measurements started at: 2011-01-15T19:57:23.918+01:00 (duration: 00:00:30)                                          |
                //| HBase cluster status: avg load: 20.00, dead servers: 0, live servers: 1, regions: 20                                 |
                //+----------------------------------------+----------+---------+--------+---------+---------+-------------+-------------+
                //| Name                                   | Op count | Average | Median | Minimum | Maximum | Alltime ops | Alltime avg |
                //+----------------------------------------+----------+---------+--------+---------+---------+-------------+-------------+
                //|-blockCacheHitRatio@lat                 |         1|    97.00|   97.00|    97.00|    97.00|            1|        97.00|
                //|-sysLoadAvg@lat                         |         1|     1.16|    1.16|     1.16|     1.16|            1|         1.16|
                //|-usedHeap@lat                           |         1|   162.90|  162.90|   162.90|   162.90|            1|       162.90|
                //|B:Blob creation                         |      1231|     0.33|    0.06|     0.02|   101.84|         1231|         0.33|
                //|Invalid messages                        |         6|     1.00|    1.00|     1.00|     1.00|            6|         1.00|
                //|C:Message record                        |      1190|    10.16|    8.30|     6.52|   444.16|         1190|        10.16|
                //|C:Part record                           |      1225|     9.29|    8.11|     6.45|    90.71|         1225|         9.29|
                //+----------------------------------------+----------+---------+--------+---------+---------+-------------+-------------+
                //| B ops/sec: 3063.80 real (=3063.80x1), 40.99 interval                                                                 |
                //| C ops/sec: 102.93 real (=102.93x1), 80.41 interval                                                                   |
                //+----------------------------------------------------------------------------------------------------------------------+

                if (test == null) {
                    test = new Test("default");
                    tests.entries.add(test);
                }

                Interval interval = new Interval(test);

                int durationPos = line.indexOf(" (");
                String ts = line.substring(INT_START_LINE.length(), durationPos);
                interval.begin = new DateTime(ts);

                // read all heading section lines
                while ((line = reader.readLine()) != null && line.startsWith("| "));

                // read the title lines (we are already positioned at the first one)
                reader.readLine();
                reader.readLine();

                // read the metrics
                while ((line = reader.readLine()) != null && line.startsWith("|")) {
                    int countColStart = line.indexOf("|", 1);
                    int averageColStart = line.indexOf("|", countColStart + 1);
                    int medianColStart = line.indexOf("|", averageColStart + 1);
                    int minColStart = line.indexOf("|", medianColStart + 1);
                    int maxColStart = line.indexOf("|", minColStart + 1);
                    int allTimeOpsColStart = line.indexOf("|", maxColStart + 1);

                    MetricData data = new MetricData();

                    String metricName = line.substring(1, countColStart).trim();

                    data.count = Integer.parseInt(line.substring(countColStart + 1, averageColStart).trim());
                    data.average = Double.parseDouble(line.substring(averageColStart + 1, medianColStart).trim());
                    data.median = Double.parseDouble(line.substring(medianColStart + 1, minColStart).trim());
                    data.min = Double.parseDouble(line.substring(minColStart + 1, maxColStart).trim());
                    data.max = Double.parseDouble(line.substring(maxColStart + 1, allTimeOpsColStart).trim());

                    interval.set(metricName, data);
                }

                // we are now on the line that closes the metrics section, ops/sec might follow (but is optional)
                while ((line = reader.readLine()) != null && line.startsWith("|")) {
                    int colonPos = line.indexOf(":");
                    // the metricsname is prefixed with a dash: indication to the reporting tool that this value
                    // only has an average
                    String metricName = "-" + line.substring(2, colonPos);
                    int realPos = line.indexOf(" real");

                    MetricData data = new MetricData();
                    data.average = Double.parseDouble(line.substring(colonPos + 2, realPos).trim());

                    interval.set(metricName, data);
                }

                // If there were no ops/sec, we might have read a line too much, in other cases it can't hurt to unread
                reader.unreadLine();

                test.add(interval);
            }
        }

        reader.close();
        reader = null;

        return tests;
    }

    public int getCurrentLine() {
        return reader != null ? reader.currentLine : -1;
    }

    public static class LineCountingReader extends BufferedReader {
        int currentLine;
        String line;
        boolean unreadLine = false;

        public LineCountingReader(Reader in) {
            super(in);
        }

        @Override
        public String readLine() throws IOException {
            if (unreadLine) {
                unreadLine = false;
                return line;
            }
            currentLine++;
            this.line = super.readLine();
            return line;
        }

        public void unreadLine() {
            unreadLine = true;
        }
    }
}
