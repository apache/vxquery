/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.xtest;

import org.kohsuke.args4j.Option;

public class XTestOptions {
    @Option(name = "-port", required = false, usage = "Port for web server to listen on")
    int port;

    @Option(name = "-xqtsbase", required = true, usage = "XQTS Base Directory")
    String xqtsBase;

    @Option(name = "-threads", required = false, usage = "Number of threads")
    int threads;

    @Option(name = "-filter", required = false, usage = "Filter regular expression")
    String filter;

    @Option(name = "-v", required = false, usage = "Verbose")
    boolean verbose;

    @Option(name = "-keepalive", required = false, usage = "Milliseconds to keep server alive after tests have completed")
    long keepalive;

    @Option(name = "-textreport", required = false, usage = "Text Report output file")
    String diffable;

    @Option(name = "-xmlreport", required = false, usage = "XML Report output file")
    String xmlReport;
}