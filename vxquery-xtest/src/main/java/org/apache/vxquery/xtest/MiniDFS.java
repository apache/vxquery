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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;

public class MiniDFS {

    private MiniDFSCluster dfsCluster;

    public void miniDFS() {

    }

    public void startHDFS() throws IOException {

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        JobConf conf = new JobConf();
        String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
        Path hdfs_conf = new Path(PATH_TO_HADOOP_CONF);
        if (!lfs.exists(hdfs_conf)) {
            PATH_TO_HADOOP_CONF = "vxquery-xtest/src/test/resources/hadoop/conf";
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

    public void shutdownHDFS() {
        System.err.println("Tests completed.Shutting down HDFS");
        dfsCluster.shutdown();
    }
}
