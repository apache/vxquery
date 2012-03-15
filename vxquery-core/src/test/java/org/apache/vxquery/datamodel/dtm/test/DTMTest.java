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
package org.apache.vxquery.datamodel.dtm.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import javax.xml.transform.stream.StreamSource;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.NameCache;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.v0datamodel.dtm.DTM;
import org.junit.Test;

public class DTMTest {
    @Test
    public void test001() throws SystemException {
        for (int i = 0; i < 10; ++i) {
            long start = System.currentTimeMillis();
            System.err.println("Starting parse");
            NameCache nameCache = new NameCache();
            AtomicValueFactory avf = new AtomicValueFactory(nameCache);
            DTM dtm = new DTM(avf, nameCache);
            GZIPInputStream in = null;
            try {
                in = new GZIPInputStream(new BufferedInputStream(new FileInputStream(new File(
                        "src/test/resources/documents/dblp.xml.gz"))));
                dtm.parse(new StreamSource(in), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.err.println("Done parse");
            System.err.println((System.currentTimeMillis() - start) + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        DTMTest test = new DTMTest();
        test.test001();
    }
}