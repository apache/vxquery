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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.xml.sax.XMLReader;

abstract class AbstractDiskTest implements IDiskTest, Runnable {
    private String filename;
    private int bufferSize;
    protected XMLReader parser;

    public void setFile(File file) {
        this.filename = file.getAbsolutePath();
    }

    public String getPrintFilename() {
        return filename.substring(filename.length() - 20);
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
        long size = -1;
        //        System.out.printf(getPrintFilename() + "\t" + getMessage() + " - Starting%n");
        long start = System.nanoTime();
        try {
            size = test(filename, bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        long timeDelta = end - start;
        double speed = 0;
        if (size > 0) {
            speed = (size * 1e3) / timeDelta;
        }
        // System.out.printf("checkSum: %d%n", checkSum);
        //        System.out.printf(getPrintFilename() + "\t" + getMessage() + "%.1f ms\t%.2f MB/s\t%.2f MB/s",
        //                (timeDelta) / 1e6, 1024 * 1024 * 1e6 / (timeDelta), speed);
        // CSV output of the results.
        try {
            System.out.printf("%s,", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.printf("%s,%s,%.1f,%.2f", filename, getMessage(), (timeDelta) / 1e6,
                1024 * 1024 * 1e6 / (timeDelta), speed);
        if (isBuffered() && bufferSize > 0) {
            System.out.printf(",%.1f", bufferSize / 1024.0);
        } else {
            System.out.printf(",0");
        }
        System.out.println();
    }

    public boolean isBuffered() {
        return true;
    }

    abstract public String getMessage();

    abstract public long test(String filename, int bufferSize) throws IOException;
}
