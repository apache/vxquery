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

package org.apache.vxquery.rest;

import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_RESULT_ENDPOINT;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.vxquery.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.service.VXQueryService;
import org.apache.vxquery.rest.servlet.QueryAPIServlet;
import org.apache.vxquery.rest.servlet.QueryResultAPIServlet;

/**
 * REST Server class responsible for starting a new server on a given port.
 *
 * @author Erandi Ganepola
 */
public class RestServer {

    public static final Logger LOGGER = Logger.getLogger(RestServer.class.getName());

    private WebManager webManager;
    private int port;

    public RestServer(VXQueryService vxQueryService, int port) {
        if (port == 0) {
            throw new IllegalArgumentException("REST Server port cannot be 0");
        }

        this.port = port;

        webManager = new WebManager();
        HttpServer restServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(), this.port);
        restServer.addServlet(new QueryAPIServlet(vxQueryService, restServer.ctx(), QUERY_ENDPOINT));
        restServer.addServlet(new QueryResultAPIServlet(vxQueryService, restServer.ctx(), QUERY_RESULT_ENDPOINT));
        webManager.add(restServer);
    }

    public void start() {
        try {
            LOGGER.log(Level.FINE, "Starting rest server on port: " + port);
            webManager.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting rest server", e);
            throw new VXQueryRuntimeException("Unable to start REST server", e);
        }
        LOGGER.log(Level.INFO, "Rest server started on port: " + port);
    }

    public void stop() {
        try {
            LOGGER.log(Level.FINE, "Stopping rest server");
            webManager.stop();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping VXQueryService", e);
            throw new VXQueryRuntimeException("Error occurred when stopping rest server", e);
        }
        LOGGER.log(Level.INFO, "Rest server stopped");
    }

    public int getPort() {
        return port;
    }
}
