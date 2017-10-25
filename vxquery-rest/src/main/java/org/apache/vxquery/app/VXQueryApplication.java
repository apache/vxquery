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

package org.apache.vxquery.app;

import static org.apache.vxquery.rest.Constants.Properties.AVAILABLE_PROCESSORS;
import static org.apache.vxquery.rest.Constants.Properties.HDFS_CONFIG;
import static org.apache.vxquery.rest.Constants.Properties.JOIN_HASH_SIZE;
import static org.apache.vxquery.rest.Constants.Properties.MAXIMUM_DATA_SIZE;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.job.resource.DefaultJobCapacityController;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.vxquery.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.RestServer;
import org.apache.vxquery.rest.service.VXQueryConfig;
import org.apache.vxquery.rest.service.VXQueryService;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Main class responsible for starting the {@link RestServer} and
 * {@link VXQueryService} classes.
 *
 * @author Erandi Ganepola
 */
public class VXQueryApplication implements ICCApplication {

    private static final Logger LOGGER = Logger.getLogger(VXQueryApplication.class.getName());

    private VXQueryService vxQueryService;
    private RestServer restServer;

    @Override
    public void start(IServiceContext ccAppCtx, String[] args) throws Exception {
        AppArgs appArgs = new AppArgs();
        if (args != null) {
            CmdLineParser parser = new CmdLineParser(appArgs);
            try {
                parser.parseArgument(args);
            } catch (Exception e) {
                parser.printUsage(System.err);
                throw new VXQueryRuntimeException("Unable to parse app arguments", e);
            }
        }

        VXQueryConfig config = loadConfiguration(ccAppCtx.getCCContext().getClusterControllerInfo(),
                appArgs.getVxqueryConfig());
        vxQueryService = new VXQueryService(config);
        restServer = new RestServer(vxQueryService, appArgs.getRestPort());
    }

    public synchronized void stop() {
        try {
            LOGGER.log(Level.INFO, "Stopping REST server");
            restServer.stop();

            LOGGER.log(Level.INFO, "Stopping VXQueryService");
            vxQueryService.stop();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping the application", e);
        }
    }

    @Override
    public void startupCompleted() throws Exception {
        try {
            LOGGER.log(Level.INFO, "Starting VXQueryService");
            vxQueryService.start();
            LOGGER.log(Level.INFO, "VXQueryService started successfully");

            LOGGER.log(Level.INFO, "Starting REST server");
            restServer.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting application", e);
            stop();
            throw new VXQueryRuntimeException("Error occurred when starting application", e);
        }
    }

    /**
     * Loads properties from
     *
     * <pre>
     * -appConfig foo/bar.properties
     * </pre>
     *
     * file if specified in the app arguments.
     *
     * @param clusterControllerInfo
     *            cluster controller information
     * @param propertiesFile
     *            vxquery configuration properties file, given by
     *
     *            <pre>
     *            -appConfig
     *            </pre>
     *
     *            option in app argument
     * @return A new {@link VXQueryConfig} instance with either default properties
     *         or properties loaded from the properties file given.
     */
    private VXQueryConfig loadConfiguration(ClusterControllerInfo clusterControllerInfo, String propertiesFile) {
        VXQueryConfig vxqConfig = new VXQueryConfig();
        if (propertiesFile != null) {
            try (InputStream in = new FileInputStream(propertiesFile)) {
                System.getProperties().load(in);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE,
                        String.format("Error occurred when loading properties file %s", propertiesFile), e);
            }
        }

        vxqConfig.setAvailableProcessors(Integer.getInteger(AVAILABLE_PROCESSORS, 1));
        vxqConfig.setJoinHashSize(Long.getLong(JOIN_HASH_SIZE, -1));
        vxqConfig.setHdfsConf(System.getProperty(HDFS_CONFIG));
        vxqConfig.setMaximumDataSize(Long.getLong(MAXIMUM_DATA_SIZE, -1));

        vxqConfig.setHyracksClientIp(clusterControllerInfo.getClientNetAddress());
        vxqConfig.setHyracksClientPort(clusterControllerInfo.getClientNetPort());

        return vxqConfig;
    }

    public VXQueryService getVxQueryService() {
        return vxQueryService;
    }

    public RestServer getRestServer() {
        return restServer;
    }

    /**
     * Application Arguments bean class
     */
    private class AppArgs {
        @Option(name = "-restPort", usage = "The port on which REST server starts")
        private int restPort = 8080;

        @Option(name = "-appConfig", usage = "Properties file location which includes VXQueryService Application additional configuration")
        private String vxqueryConfig = null;

        public String getVxqueryConfig() {
            return vxqueryConfig;
        }

        public void setVxqueryConfig(String vxqueryConfig) {
            this.vxqueryConfig = vxqueryConfig;
        }

        public int getRestPort() {
            return restPort;
        }

        public void setRestPort(int restPort) {
            this.restPort = restPort;
        }
    }

    @Override
    public Object getApplicationContext() {
        return null;
    }

    @Override
    public void registerConfig(IConfigManager configManager) {
        configManager.addIniParamOptions(ControllerConfig.Option.CONFIG_FILE, ControllerConfig.Option.CONFIG_FILE_URL);
        configManager.addCmdLineSections(Section.CC, Section.COMMON);
        configManager.setUsageFilter(getUsageFilter());
        configManager.register(ControllerConfig.Option.class, CCConfig.Option.class, NCConfig.Option.class);
    }

    @Override
    public IJobCapacityController getJobCapacityController() {
        return DefaultJobCapacityController.INSTANCE;
    }
}
