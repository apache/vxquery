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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;
import org.mortbay.jetty.Server;

public class XTest {
    private XTestOptions opts;
    private Server server;
    private ExecutorService eSvc;
    private List<ResultReporter> reporters;
    private TestRunnerFactory trf;
    private int count;
    private int finishCount;
    private MiniDFSCluster dfsCluster;

    XTest(XTestOptions opts) {
        this.opts = opts;
        reporters = new ArrayList<ResultReporter>();
    }

    void init() throws Exception {

        finishCount = 0;
        if (opts.threads <= 0) {
            opts.threads = 1;
        }
        eSvc = Executors.newFixedThreadPool(opts.threads);
        if (opts.port > 0) {
            ServletReporterImpl servletHandler = new ServletReporterImpl();
            reporters.add(servletHandler);
            server = new Server(opts.port);
            server.addHandler(servletHandler);
            server.start();
        }
        if (opts.htmlReport != null) {
            reporters.add(new HTMLFileReporterImpl(new File(opts.htmlReport)));
        }
        if (opts.xmlReport != null) {
            reporters.add(new XMLFileReporterImpl(new File(opts.xmlReport)));
        }
        if (opts.diffable != null) {
            reporters.add(new LineFileReporterImpl(new File(opts.diffable)));
        }
        reporters.add(new ResultReporter() {
            @Override
            public void close() {
            }

            @Override
            public void reportResult(TestCaseResult result) {
                synchronized (XTest.this) {
                    finishCount++;
                    if (finishCount >= count) {
                        XTest.this.notifyAll();
                    }
                }
            }
        });
        trf = new TestRunnerFactory(opts);
        //setupHDFS();
        trf.registerReporters(reporters);
        TestCaseFactory tcf = new TestCaseFactory(trf, eSvc, opts);
        count = tcf.process();

    }

    synchronized void waitForCompletion() throws InterruptedException {
        while (finishCount < count) {
            wait();
        }
        if (opts.keepalive > 0) {
            Thread.sleep(opts.keepalive);
        }
        eSvc.shutdown();
        while (!eSvc.awaitTermination(5L, TimeUnit.SECONDS)) {
            System.err.println("Failed to close all threads, trying again...");
        }
        for (ResultReporter r : reporters) {
            r.close();
        }
        try {
            eSvc.awaitTermination(opts.keepalive, TimeUnit.MILLISECONDS);
        } finally {
            try {
                if (server != null) {
                    server.stop();
                }
                trf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setupHDFS() throws IOException {
        
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        JobConf conf = new JobConf();
        String PATH_TO_HADOOP_CONF = "vxquery-xtest/src/test/resources/hadoop/conf";
        Path hdfs_conf = new Path(PATH_TO_HADOOP_CONF);
        if (!lfs.exists(hdfs_conf)) {
            PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
            hdfs_conf = new Path(PATH_TO_HADOOP_CONF);
            if (!lfs.exists(hdfs_conf)) {
                PATH_TO_HADOOP_CONF = "../vxquery-xtest/src/test/resources/hadoop/conf";
                hdfs_conf = new Path(PATH_TO_HADOOP_CONF);
            }
        }
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        int numDataNodes = 1;
        int nameNodePort = 40000;

        // cleanup artifacts created on the local file system
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        MiniDFSCluster.Builder build = new MiniDFSCluster.Builder(conf);
        build.nameNodePort(nameNodePort);
        build.nameNodeHttpPort(nameNodePort + 34);
        build.numDataNodes(numDataNodes);
        build.checkExitOnShutdown(true);
        build.startupOption(StartupOption.REGULAR);
        build.format(true);
        build.waitSafeMode(true);
        dfsCluster = build.build();

        FileSystem dfs = FileSystem.get(conf);
        String DATA_PATH = "src/test/resources/TestSources/ghcnd";
        Path src = new Path(DATA_PATH);
        if (!lfs.exists(src)) {
            DATA_PATH = "vxquery-xtest/src/test/resources/TestSources/ghcnd";
            src = new Path(DATA_PATH);
            if (!lfs.exists(src)) {
                DATA_PATH = "../vxquery-xtest/src/test/resources/TestSources/ghcnd";
                src = new Path(DATA_PATH);
            }
        }
        Path dest = new Path("vxquery-hdfs-test");
        dfs.copyFromLocalFile(src, dest);
        if (dfs.exists(dest)) {
            System.err.println("Test files copied to HDFS successfully");
        }
    }

    public void shutdownDFS() {
        System.err.println("Tests completed.Shutting down HDFS");
        dfsCluster.shutdown();
    }
}
