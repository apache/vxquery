/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.cli;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.LogManager;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.app.util.LocalClusterUtil;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.apache.vxquery.rest.response.Metrics;
import org.apache.vxquery.rest.response.SyncQueryResponse;
import org.apache.vxquery.rest.service.VXQueryConfig;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * CLI for VXQuery. This class is using the REST API to execute statements given by the user.
 *
 * @author Erandi Ganepola
 */
public class VXQuery {

    private final CmdLineOptions opts;

    private static LocalClusterUtil localClusterUtil;
    private String restIpAddress;
    private int restPort;

    private static List<Metrics> metricsList = new ArrayList<>();
    private int executionIteration;

    /**
     * Constructor to use command line options passed.
     *
     * @param opts
     *            Command line options object
     */
    public VXQuery(CmdLineOptions opts) {
        this.opts = opts;
    }

    /**
     * Main method to get command line options and execute query process.
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        LogManager.getLogManager().reset();

        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);

        // parse command line options, give error message if no arguments passed
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }

        if (opts.xqFiles.isEmpty()) {
            parser.printUsage(System.err);
            return;
        }

        VXQuery vxq = new VXQuery(opts);
        vxq.execute(opts.xqFiles);
    }

    private void execute(List<String> xqFiles) {
        if (opts.restIpAddress == null) {
            System.out.println("No REST Ip address given. Creating a local hyracks cluster");

            VXQueryConfig vxqConfig = new VXQueryConfig();
            vxqConfig.setAvailableProcessors(opts.availableProcessors);
            vxqConfig.setFrameSize(opts.frameSize);
            vxqConfig.setHdfsConf(opts.hdfsConf);
            vxqConfig.setJoinHashSize(opts.joinHashSize);
            vxqConfig.setMaximumDataSize(opts.maximumDataSize);

            localClusterUtil = new LocalClusterUtil();
            try {
                localClusterUtil.init(vxqConfig);
                restIpAddress = localClusterUtil.getIpAddress();
                restPort = vxqConfig.getRestApiPort();
            } catch (Exception e) {
                System.err.println("Unable to start local hyracks cluster due to: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        } else {
            restIpAddress = opts.restIpAddress;
            restPort = opts.restPort;
        }

        System.out.println("Running queries given in: " + Arrays.toString(xqFiles.toArray()));
        runQueries(xqFiles);

        if (localClusterUtil != null) {
            try {
                localClusterUtil.deinit();
            } catch (Exception e) {
                System.err.println("Error occurred when stopping local hyracks: " + e.getMessage());
            }
        }
    }

    public void runQueries(List<String> xqFiles) {
        for (String xqFile : xqFiles) {
            String query;
            try {
                query = slurp(xqFile);
            } catch (IOException e) {
                System.err.println(String.format("Error occurred when reading XQuery file %s with message: %s", xqFile,
                        e.getMessage()));
                continue;
            }

            System.out.println();
            System.out.println("====================================================");
            System.out.println("\tQuery - '" + xqFile + "'");
            System.out.println("====================================================");

            QueryRequest request = createQueryRequest(opts, query);
            metricsList.clear();

            for (int i = 0; i < opts.repeatExec; i++) {
                System.out.println("**** Repetition : " + (i + 1) + " ****");

                executionIteration = i;
                sendQueryRequest(xqFile, request, this);
            }

            if (opts.repeatExec > 1) {
                showTimingSummary();
            }
        }
    }

    private void onSuccess(String xqFile, QueryRequest request, SyncQueryResponse response) {
        if (response == null) {
            System.err.println(String.format("Unable to execute query %s", request.getStatement()));
            return;
        }

        if (opts.showQuery) {
            printField("Query", response.getStatement());
        }

        if (request.isShowMetrics()) {
            String metrics = String.format("Compile Time:\t%d\nElapsed Time:\t%d",
                    response.getMetrics().getCompileTime(), response.getMetrics().getElapsedTime());
            printField("Metrics", metrics);
        }

        if (request.isShowAbstractSyntaxTree()) {
            printField("Abstract Syntax Tree", response.getAbstractSyntaxTree());
        }

        if (request.isShowTranslatedExpressionTree()) {
            printField("Translated Expression Tree", response.getTranslatedExpressionTree());
        }

        if (request.isShowOptimizedExpressionTree()) {
            printField("Optimized Expression Tree", response.getOptimizedExpressionTree());
        }

        if (request.isShowRuntimePlan()) {
            printField("Runtime Plan", response.getRuntimePlan());
        }

        printField("Results", response.getResults());

        if (executionIteration >= opts.timingIgnoreQueries) {
            metricsList.add(response.getMetrics());
        }
    }

    private void onFailure(String xqFile, ErrorResponse response) {
        if (response == null) {
            System.err.println(String.format("Unable to execute query in %s", xqFile));
            return;
        }

        System.err.println();
        System.err.println("------------------------ Errors ---------------------");

        Error error = response.getError();
        String errorMsg = String.format("Code:\t %d\nMessage:\t %s", error.getCode(), error.getMessage());
        printField(System.err, String.format("Errors for '%s'", xqFile), errorMsg);
    }

    /**
     * Submits a query to be executed by the REST API. Will call {@link #onFailure(String, ErrorResponse)} if any error
     * occurs when submitting the query. Else will call {@link #onSuccess(String, QueryRequest, SyncQueryResponse)} with
     * the {@link AsyncQueryResponse}
     *
     * @param xqFile
     *            .xq file with the query to be executed
     * @param request
     *            {@link QueryRequest} instance to be submitted to REST API
     * @param cli
     *            cli class instance
     */
    private static void sendQueryRequest(String xqFile, QueryRequest request, VXQuery cli) {
        URI uri = null;
        try {
            uri = RestUtils.buildQueryURI(request, cli.restIpAddress, cli.restPort);
        } catch (URISyntaxException e) {
            System.err.println(
                    String.format("Unable to build URI to call REST API for query: %s", request.getStatement()));
            cli.onFailure(xqFile, null);
        }

        CloseableHttpClient httpClient = HttpClients.custom().build();
        try {
            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader(HttpHeaders.ACCEPT, CONTENT_TYPE_JSON);

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                HttpEntity entity = httpResponse.getEntity();

                String response = RestUtils.readEntity(entity);
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    cli.onSuccess(xqFile, request,
                            RestUtils.mapEntity(response, SyncQueryResponse.class, CONTENT_TYPE_JSON));
                } else {
                    cli.onFailure(xqFile, RestUtils.mapEntity(response, ErrorResponse.class, CONTENT_TYPE_JSON));
                }
            } catch (IOException e) {
                System.err.println("Error occurred when reading entity: " + e.getMessage());
                cli.onFailure(xqFile, null);
            } catch (JAXBException e) {
                System.err.println("Error occurred when mapping query response: " + e.getMessage());
                cli.onFailure(xqFile, null);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    /**
     * Once the query in a given .xq file has been executed (with repeated executions as well), this method calculates
     * mean, standard deviation, minimum and maximum execution times.
     */
    private void showTimingSummary() {
        double sumTime = 0;
        double sumSquaredTime = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        for (int i = 0; i < metricsList.size(); i++) {
            Metrics metrics = metricsList.get(i);
            long totalTime = metrics.getCompileTime() + metrics.getElapsedTime();

            sumTime += totalTime;
            sumSquaredTime += totalTime * totalTime;

            if (totalTime < minTime) {
                minTime = totalTime;
            }

            if (totalTime > maxTime) {
                maxTime = totalTime;
            }
        }

        double mean = sumTime / (opts.repeatExec - opts.timingIgnoreQueries);
        double sd = Math.sqrt(sumSquaredTime / (opts.repeatExec - opts.timingIgnoreQueries) - mean * mean);

        System.out.println();
        System.out.println("\t**** Timing Summary ****");
        System.out.println("----------------------------------------------------");
        System.out.println(String.format("Repetitions:\t%d, Timing Ignored Iterations:\t%d", opts.repeatExec,
                opts.timingIgnoreQueries));
        System.out.println("Average execution time:\t" + mean + " ms");
        System.out.println("Standard deviation:\t" + String.format("%.4f", sd));
        System.out.println("Coefficient of variation:\t" + String.format("%.4f", sd / mean));
        System.out.println("Minimum execution time:\t" + minTime + " ms");
        System.out.println("Maximum execution time:\t" + maxTime + " ms");
        System.out.println();
    }

    private static QueryRequest createQueryRequest(CmdLineOptions opts, String query) {
        QueryRequest request = new QueryRequest(query);
        request.setCompileOnly(opts.compileOnly);
        request.setOptimization(opts.optimizationLevel);
        request.setFrameSize(opts.frameSize);
        request.setRepeatExecutions(opts.repeatExec);
        request.setShowMetrics(opts.timing);
        request.setShowAbstractSyntaxTree(opts.showAST);
        request.setShowTranslatedExpressionTree(opts.showTET);
        request.setShowOptimizedExpressionTree(opts.showOET);
        request.setShowRuntimePlan(opts.showRP);
        request.setAsync(false);

        return request;
    }

    /**
     * Reads the contents of file given in query into a String. The file is always closed. For XML files UTF-8 encoding
     * is used.
     *
     * @param query
     *            The query with filename to be processed
     * @return UTF-8 formatted query string
     * @throws IOException
     */
    private static String slurp(String query) throws IOException {
        return FileUtils.readFileToString(new File(query), "UTF-8");
    }

    private static void printField(PrintStream out, String field, String value) {
        out.println();
        field = field + ":";
        out.print(field);

        String[] lines = value.split("\n");
        for (int i = 0; i < lines.length; i++) {
            int margin = 4;
            if (i != 0) {
                margin += field.length();
            }
            System.out.print(String.format("%1$" + margin + "s%2$s\n", "", lines[i]));
        }
    }

    private static void printField(String field, String value) {
        printField(System.out, field, value);
    }

    /**
     * Helper class with fields and methods to handle all command line options
     */
    private static class CmdLineOptions {
        @Option(name = "-rest-ip-address", usage = "IP Address of the REST Server")
        private String restIpAddress = null;

        @Option(name = "-rest-port", usage = "Port of REST Server")
        private int restPort = 8085;

        @Option(name = "-compileonly", usage = "Compile the query and stop.")
        private boolean compileOnly;

        @Option(name = "-O", usage = "Optimization Level. (default: Full Optimization)")
        private int optimizationLevel = Integer.MAX_VALUE;

        @Option(name = "-frame-size", usage = "Frame size in bytes. (default: 65,536)")
        private int frameSize = 65536;

        @Option(name = "-repeatexec", usage = "Number of times to repeat execution.")
        private int repeatExec = 1;

        @Option(name = "-timing", usage = "Produce timing information.")
        private boolean timing;

        @Option(name = "-showquery", usage = "Show query string.")
        private boolean showQuery;

        @Option(name = "-showast", usage = "Show abstract syntax tree.")
        private boolean showAST;

        @Option(name = "-showtet", usage = "Show translated expression tree.")
        private boolean showTET;

        @Option(name = "-showoet", usage = "Show optimized expression tree.")
        private boolean showOET;

        @Option(name = "-showrp", usage = "Show Runtime plan.")
        private boolean showRP;

        // Optional (Not supported by REST API) parameters. Only used for creating a
        // local hyracks cluster
        @Option(name = "-join-hash-size", usage = "Join hash size in bytes. (default: 67,108,864)")
        private long joinHashSize = -1;

        @Option(name = "-maximum-data-size", usage = "Maximum possible data size in bytes. (default: 150,323,855,000)")
        private long maximumDataSize = -1;

        @Option(name = "-buffer-size", usage = "Disk read buffer size in bytes.")
        private int bufferSize = -1;

        @Option(name = "-result-file", usage = "File path to save the query result.")
        private String resultFile = null;

        @Option(name = "-timing-ignore-queries", usage = "Ignore the first X number of quereies.")
        private int timingIgnoreQueries = 0;

        @Option(name = "-hdfs-conf", usage = "Directory path to Hadoop configuration files")
        private String hdfsConf = null;

        @Option(name = "-available-processors", usage = "Number of available processors. (default: java's available processors)")
        private int availableProcessors = -1;

        @Option(name = "-local-node-controllers", usage = "Number of local node controllers. (default: 1)")
        private int localNodeControllers = 1;

        @Argument
        private List<String> xqFiles = new ArrayList<>();
    }
}
