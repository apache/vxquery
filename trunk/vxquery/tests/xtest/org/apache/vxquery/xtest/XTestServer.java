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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mortbay.jetty.Server;

public class XTestServer {
    private XTestCmdLineOptions opts;
    private Server server;
    private ExecutorService eSvc;

    XTestServer(XTestCmdLineOptions opts) {
        this.opts = opts;
    }

    void init() throws Exception {
        eSvc = Executors.newFixedThreadPool(opts.threads);
        Reporter reporter = new Reporter();
        TestRunnerFactory trf = new TestRunnerFactory(reporter);
        TestCaseFactory tcf = new TestCaseFactory(opts.xqtsBase, trf, eSvc, opts);
        server = new Server(opts.port);
        server.addHandler(reporter);
        server.start();
        tcf.process();
    }

    void waitForCompletion() throws InterruptedException {
        try {
            eSvc.awaitTermination(10000, TimeUnit.SECONDS);
        } finally {
            try {
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}