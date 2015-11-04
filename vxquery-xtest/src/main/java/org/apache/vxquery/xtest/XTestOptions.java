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
package org.apache.vxquery.xtest;

import org.kohsuke.args4j.Option;

public class XTestOptions {
    @Option(name = "-O", required = false, usage = "Optimization Level")
    int optimizationLevel = Integer.MAX_VALUE;

    @Option(name = "-port", required = false, usage = "Port for web server to listen on")
    int port;

    @Option(name = "-catalog", required = true, usage = "Test Catalog XML")
    String catalog;

    @Option(name = "-threads", required = false, usage = "Number of threads")
    int threads;

    @Option(name = "-include", required = false, usage = "Include filter regular expression")
    String include;

    @Option(name = "-exclude", required = false, usage = "Exclude filter regular expression")
    String exclude;

    @Option(name = "-previous-test-results", required = false, usage = "File path to previous test results (text report output file)")
    String previousTestResults;

    @Option(name = "-v", required = false, usage = "Verbose")
    boolean verbose;

    @Option(name = "-keepalive", required = false, usage = "Milliseconds to keep server alive after tests have completed")
    long keepalive;

    @Option(name = "-textreport", required = false, usage = "Text Report output file")
    String diffable;

    @Option(name = "-xmlreport", required = false, usage = "XML Report output file")
    String xmlReport;

    @Option(name = "-htmlreport", required = false, usage = "HTML Report output file")
    String htmlReport;
}