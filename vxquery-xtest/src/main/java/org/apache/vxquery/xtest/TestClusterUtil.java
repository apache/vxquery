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

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestClusterUtil {

    private static final int CLIENT_NET_PORT = 39000;
    private static final int CLUSTER_NET_PORT = 39001;
    private static final int PROFILE_DUMP_PERIOD = 10000;
    private static final String CC_HOST = "localhost";
    private static final String NODE_ID = "nc1";
    private static final String IO_DEVICES = "target/tmp/indexFolder";

    private static HyracksConnection hcc;
    private static HyracksDataset hds;

    private TestClusterUtil() {
    }

    public static CCConfig createCCConfig() throws UnknownHostException {
        String publicAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress(publicAddress);
        ccConfig.setClientListenPort(CLIENT_NET_PORT);
        ccConfig.setClusterListenAddress(publicAddress);
        ccConfig.setClusterListenPort(CLUSTER_NET_PORT);
        ccConfig.setProfileDumpPeriod(PROFILE_DUMP_PERIOD);
        return ccConfig;
    }

    public static NCConfig createNCConfig() throws UnknownHostException {
        String publicAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        NCConfig ncConfig1 = new NCConfig(NODE_ID);
        ncConfig1.setClusterAddress(CC_HOST);
        ncConfig1.setClusterPort(CLUSTER_NET_PORT);
        ncConfig1.setClusterListenAddress(publicAddress);
        ncConfig1.setDataPublicAddress(publicAddress);
        ncConfig1.setResultPublicAddress(publicAddress);
        ncConfig1.setIODevices(new String[] { IO_DEVICES });
        return ncConfig1;
    }

    public static ClusterControllerService startCC(XTestOptions opts) throws IOException {
        CCConfig ccConfig = createCCConfig();
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(TestRunner.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        try {
            ClusterControllerService cc = new ClusterControllerService(ccConfig);
            cc.start();
            hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
            hds = new HyracksDataset(hcc, opts.frameSize, opts.threads);
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

    public static HyracksConnection getConnection() {
        return hcc;
    }

    public static HyracksDataset getDataset() {
        return hds;
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
