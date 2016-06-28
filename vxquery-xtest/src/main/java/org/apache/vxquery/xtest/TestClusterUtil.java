/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public class TestClusterUtil {

    private static int clientNetPort = 39000;
    private static int clusterNetPort = 39001;
    private static int profileDumpPeriod = 10000;
    private static String ccHost = "localhost";
    private static String nodeId = "nc1";

    public static CCConfig createCCConfig() throws UnknownHostException {
        String publicAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = publicAddress;
        ccConfig.clientNetPort = clientNetPort;
        ccConfig.clusterNetIpAddress = publicAddress;
        ccConfig.clusterNetPort = clusterNetPort;
        ccConfig.profileDumpPeriod = profileDumpPeriod;
        return ccConfig;
    }

    public static NCConfig createNCConfig() throws UnknownHostException {
        String publicAddress = InetAddress.getLocalHost().getHostAddress();
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = ccHost;
        ncConfig1.ccPort = clusterNetPort;
        ncConfig1.clusterNetIPAddress = publicAddress;
        ncConfig1.dataIPAddress = publicAddress;
        ncConfig1.resultIPAddress = publicAddress;
        ncConfig1.nodeId = nodeId;
        return ncConfig1;
    }

    public static ClusterControllerService startCC() throws IOException {
        CCConfig ccConfig = createCCConfig();
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(TestRunner.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        try {
            ClusterControllerService cc = new ClusterControllerService(ccConfig);
            cc.start();
            return cc;
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    public static NodeControllerService startNC() throws IOException {
        NCConfig ncConfig = createNCConfig();
        try {
            NodeControllerService nc = new NodeControllerService(ncConfig);
            nc.start();
            return nc;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static void stopCluster(ClusterControllerService cc, NodeControllerService nc) throws IOException {
        try {
            nc.stop();
            cc.stop();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
