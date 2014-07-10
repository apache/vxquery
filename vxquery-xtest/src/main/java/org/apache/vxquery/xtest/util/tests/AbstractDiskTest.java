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
package org.apache.vxquery.xtest.util.tests;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import org.xml.sax.XMLReader;

abstract class AbstractDiskTest implements IDiskTest, Runnable {
    private String filename;
    private int bufferSize;
    protected XMLReader parser;

    public AbstractDiskTest() {
    }

    public AbstractDiskTest(String filename, int bufferSize) {
        setFilename(filename);
        setBufferSize(bufferSize);
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setParser(XMLReader parser) {
        this.parser = parser;
    }

    public void start() {
        run();
    }

    public void run() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        System.out.printf(filename + "\t" + getMessage() + " - Starting%n");
        long start = System.nanoTime();
        try {
            long checkSum = test(filename, bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        // System.out.printf("checkSum: %d%n", checkSum);
        System.out.printf(filename + "\t" + getMessage() + "%.2f MB/s", 1024 * 1e9 / (end - start));
        if (isBuffered() && bufferSize > 0) {
            System.out.printf("\t%.1f KB buffer", bufferSize / 1024.0);
        }
        System.out.println();
        System.out.printf("%.2f%% load average last minute%n", os.getSystemLoadAverage());
    }

    public boolean isBuffered() {
        return true;
    }

    abstract public String getMessage();

    abstract public long test(String filename, int bufferSize) throws IOException;
}
