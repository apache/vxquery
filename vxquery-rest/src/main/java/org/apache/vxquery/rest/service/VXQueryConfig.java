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

package org.apache.vxquery.rest.service;

/**
 * A class to store default/user specified configurations required at runtime by
 * the {@link VXQueryService} class. These configuration will be loaded through
 * a properties file.
 *
 * @author Erandi Ganepola
 */
public class VXQueryConfig {

    /** Number of available processors. (default: java's available processors) */
    private int availableProcessors = Runtime.getRuntime().availableProcessors();
    /** Setting frame size. (default: 65,536) */
    private int frameSize = 65536;
    /** Join hash size in bytes. (default: 67,108,864) */
    private long joinHashSize = -1;
    /** Maximum possible data size in bytes. (default: 150,323,855,000) */
    private long maximumDataSize = -1;
    /** Directory path to Hadoop configuration files */
    private String hdfsConf = null;


    private String hyracksClientIp;
    private int hyracksClientPort;
    private int restApiPort = 39003;

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        if (availableProcessors > 0) {
            this.availableProcessors = availableProcessors;
        }
    }

    public long getJoinHashSize() {
        return joinHashSize;
    }

    public void setJoinHashSize(long joinHashSize) {
        this.joinHashSize = joinHashSize;
    }

    public long getMaximumDataSize() {
        return maximumDataSize;
    }

    public void setMaximumDataSize(long maximumDataSize) {
        this.maximumDataSize = maximumDataSize;
    }

    public String getHdfsConf() {
        return hdfsConf;
    }

    public void setHdfsConf(String hdfsConf) {
        this.hdfsConf = hdfsConf;
    }

    public int getHyracksClientPort() {
        return hyracksClientPort;
    }

    public void setHyracksClientPort(int hyracksClientPort) {
        this.hyracksClientPort = hyracksClientPort;
    }

    public String getHyracksClientIp() {
        return hyracksClientIp;
    }

    public void setHyracksClientIp(String hyracksClientIp) {
        this.hyracksClientIp = hyracksClientIp;
    }

    public int getRestApiPort() {
        return restApiPort;
    }

    public void setRestApiPort(int port) {
        this.restApiPort = port;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

}
