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
package org.apache.vxquery.cli;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class VXQueryClusterShutdown {
    /**
     * Main method to get command line options and execute query process.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);

        // parse command line options
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        
        // give error message if missing arguments
        if (opts.clientNetIpAddress == null) {
            parser.printUsage(System.err);
            return;
        }
        
        try {
            IHyracksClientConnection hcc = new HyracksConnection(opts.clientNetIpAddress, opts.clientNetPort);
            hcc.stopCluster();
        } catch (Exception e) {
            System.err.println("Unable to connect and shutdown the Hyracks cluster.");
            System.err.println(e);
            return;
        }
    }

    /**
     * Helper class with fields and methods to handle all command line options
     */
    private static class CmdLineOptions {
        @Option(name = "-client-net-ip-address", usage = "IP Address of the ClusterController", required = true)
        private String clientNetIpAddress;

        @Option(name = "-client-net-port", usage = "Port of the ClusterController")
        private int clientNetPort = 1098;

        @Argument
        private List<String> arguments = new ArrayList<String>();
    }

}
