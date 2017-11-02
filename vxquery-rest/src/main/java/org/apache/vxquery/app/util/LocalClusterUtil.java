/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.vxquery.app.util;

import static org.apache.vxquery.rest.Constants.Properties.AVAILABLE_PROCESSORS;
import static org.apache.vxquery.rest.Constants.Properties.HDFS_CONFIG;
import static org.apache.vxquery.rest.Constants.Properties.JOIN_HASH_SIZE;
import static org.apache.vxquery.rest.Constants.Properties.MAXIMUM_DATA_SIZE;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.List;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.rest.service.VXQueryConfig;
import org.apache.vxquery.rest.service.VXQueryService;

/**
 * A utility class to start the a local hyracks cluster.
 *
 * @author Preston Carman
 */
public class LocalClusterUtil {
    /*
     * Start local virtual cluster with cluster controller node and node controller
     * nodes. IP address provided for node controller is localhost. Unassigned ports
     * 39000 and 39001 are used for client and cluster port respectively.
     */
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 39000;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 39001;
    public static final int DEFAULT_HYRACKS_CC_HTTP_PORT = 39002;
    public static final int DEFAULT_VXQUERY_REST_PORT = 39003;

    // TODO review variable scope after XTest is updated to use the REST service.
    private ClusterControllerService clusterControllerService;
    private NodeControllerService nodeControllerSerivce;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;
    private ConfigManager configManager;
    private VXQueryService vxQueryService;
    private List<String> nodeNames;

    public void init(VXQueryConfig config) throws Exception {
        final ICCApplication ccApplication = createCCApplication();
        configManager = new ConfigManager();
        ccApplication.registerConfig(configManager);
        // Following properties are needed by the app to setup
        System.setProperty(AVAILABLE_PROCESSORS, String.valueOf(config.getAvailableProcessors()));
        System.setProperty(JOIN_HASH_SIZE, String.valueOf(config.getJoinHashSize()));
        System.setProperty(MAXIMUM_DATA_SIZE, String.valueOf(config.getMaximumDataSize()));
        if (config.getHdfsConf() != null) {
            System.setProperty(HDFS_CONFIG, config.getHdfsConf());
        }

        // Cluster controller
        CCConfig ccConfig = createCCConfig(configManager);
        clusterControllerService = new ClusterControllerService(ccConfig, ccApplication);
        nodeNames = ccConfig.getConfigManager().getNodeNames();
        for (String nodeId : nodeNames) {
            // mark this NC as virtual in the CC's config manager, so he doesn't try to contact NCService...
            configManager.set(nodeId, NCConfig.Option.NCSERVICE_PORT, NCConfig.NCSERVICE_PORT_DISABLED);
        }
        clusterControllerService.start();

        // hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        hcc = new HyracksConnection(clusterControllerService.getConfig().getClientListenAddress(),
                clusterControllerService.getConfig().getClientListenPort());
        hds = new HyracksDataset(hcc, config.getFrameSize(), config.getAvailableProcessors());

        // Node controller
        NCConfig ncConfig = createNCConfig();
        nodeControllerSerivce = new NodeControllerService(ncConfig);
        nodeControllerSerivce.start();

        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());

        // REST controller
        config.setHyracksClientIp(ccConfig.getClientListenAddress());
        config.setHyracksClientPort(ccConfig.getClientListenPort());
        vxQueryService = new VXQueryService(config);
        vxQueryService.start();
    }

    protected ICCApplication createCCApplication() {
        return new VXQueryApplication();
    }

    protected CCConfig createCCConfig(ConfigManager configManager) throws IOException {
        String localAddress = getIpAddress();
        CCConfig ccConfig = new CCConfig(configManager);
        ccConfig.setClientListenAddress(localAddress);
        ccConfig.setClientListenPort(DEFAULT_HYRACKS_CC_CLIENT_PORT);
        ccConfig.setClusterListenAddress(localAddress);
        ccConfig.setClusterListenPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ccConfig.setConsoleListenPort(DEFAULT_HYRACKS_CC_HTTP_PORT);
        ccConfig.setProfileDumpPeriod(10000);
       // configManager.set(ControllerConfig.Option.DEFAULT_DIR, joinPath(getDefaultStoragePath(), "asterixdb"));
        // ccConfig.setAppClass(VXQueryApplication.class.getName());
        //ccConfig.appArgs = Arrays.asList("-restPort", String.valueOf(DEFAULT_VXQUERY_REST_PORT));

        return ccConfig;
    }

    protected NCConfig createNCConfig() throws IOException {
        String localAddress = getIpAddress();
        NCConfig ncConfig = new NCConfig("test_node");
        ncConfig.setClusterAddress("localhost");
        ncConfig.setClusterPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ncConfig.setClusterListenAddress(localAddress);
        ncConfig.setDataListenAddress(localAddress);
        ncConfig.setResultListenAddress(localAddress);
        //TODO: enable index folder as a cli option for on-the-fly indexing queries
        ncConfig.setIODevices(new String[] { Files.createTempDirectory(ncConfig.getNodeId()).toString() });
        return ncConfig;
    }

    public IHyracksClientConnection getHyracksClientConnection() {
        return hcc;
    }

    public VXQueryService getVxQueryService() {
        return vxQueryService;
    }

    public void deinit() throws Exception {
        vxQueryService.stop();
        nodeControllerSerivce.stop();
        clusterControllerService.stop();
    }

    public static void main(String[] args) {
        LocalClusterUtil localClusterUtil = new LocalClusterUtil();
        VXQueryConfig config = new VXQueryConfig();
        run(localClusterUtil, config);
    }

    protected static void run(final LocalClusterUtil localClusterUtil, VXQueryConfig config) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    localClusterUtil.deinit();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            localClusterUtil.init(config);
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public String getIpAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress();
    }

    public int getRestPort() {
        return DEFAULT_VXQUERY_REST_PORT;
    }

    @Deprecated
    public IHyracksClientConnection getConnection() {
        return hcc;
    }

    @Deprecated
    public IHyracksDataset getDataset() {
        return hds;
    }

}