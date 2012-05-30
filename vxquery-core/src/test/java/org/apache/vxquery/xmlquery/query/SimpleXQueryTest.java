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
package org.apache.vxquery.xmlquery.query;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.zip.GZIPInputStream;

import junit.framework.Assert;

import org.apache.vxquery.api.InternalAPI;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMSequence;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.v0datamodel.dtm.DTMDatamodelStaticInterfaceImpl;
import org.apache.vxquery.v0runtime.base.CloseableIterator;
import org.apache.vxquery.v0runtime.base.OpenableCloseableIterator;
import org.junit.Test;

public class SimpleXQueryTest {
    @Test
    public void simple001() {
        runTest("simple001", "for $x in (1, 2.0, 3) return $x");
    }

    @Test
    public void simple002() {
        runTest("simple002", "fn:true()");
    }

    @Test
    public void simple003() {
        runTest("simple003", "fn:false()");
    }

    @Test
    public void simple004() {
        runTest("simple004", "fn:exists(())");
    }

    @Test
    public void simple005() {
        runTest("simple005", "fn:exists((1, 2))");
    }

    @Test
    public void simple006() {
        runTest("simple006", "fn:boolean(())");
    }

    @Test
    public void simple007() {
        runTest("simple007", "fn:boolean((fn:false()))");
    }

    @Test
    public void simple008() {
        runNegTest("simple008", "fn:boolean((fn:false(), fn:false()))");
    }

    @Test
    public void simple009() {
        runTest("simple009", "fn:boolean((fn:true()))");
    }

    @Test
    public void simple010() {
        runTest("simple010", "for $x in (1, 2, 3), $y in ('a', 'b', 'c') for $z in (1, 2) return ($x, $y, $z)");
    }

    @Test
    public void simple011() {
        runTest("simple011", "for $x in (12e-2, 2.3, 54.2) return fn:ceiling($x)");
    }

    @Test
    public void simple012() {
        runTest("simple012", "fn:codepoints-to-string((2309, 2358, 2378, 2325))");
    }

    @Test
    public void simple013() {
        runTest("simple013", "fn:string-to-codepoints(fn:codepoints-to-string((2309, 2358, 2378, 2325)))");
    }

    @Test
    public void simple014() {
        runTest("simple014", "normalize-space('        This                 \nis great!       ')");
    }

    @Test
    public void simple015() {
        runTest("simple015", "encode-for-uri('http://www.example.com/00/Weather/CA/Los%20Angeles#ocean')");
    }

    @Test
    public void simple016() {
        // TODO unzipping every time is a little slow ...
        String temp = gunzip("src/test/resources/documents/", "dblp.xml");
        runTest("simple016", "string-length(doc('" + temp + "'))");
    }

    private static String gunzip(String dir, String filename) {
        try {
            GZIPInputStream in = new GZIPInputStream(new BufferedInputStream(new FileInputStream(new File(dir
                    + filename + ".gz"))));
            File temp = File.createTempFile("vxquery", filename);
            temp.deleteOnExit();
            FileOutputStream out = new FileOutputStream(temp);
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            in.close();
            out.close();
            return temp.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runTest(String testName, String query) {
        try {
            runTestInternal(testName, query);
        } catch (SystemException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private static void runNegTest(String testName, String query) {
        try {
            runTestInternal(testName, query);
            Assert.fail();
        } catch (SystemException e) {
            e.printStackTrace();
        }
    }

    private static void runTestInternal(String testName, String query) throws SystemException {
        InternalAPI iapi = new InternalAPI(new DTMDatamodelStaticInterfaceImpl());
        OpenableCloseableIterator ri = iapi.execute(iapi.compile(null, iapi.parse(testName, new StringReader(query)),
                Integer.MAX_VALUE));
        ri.open();
        System.err.println("--- Results begin");
        XDMValue o;
        try {
            while ((o = (XDMValue) ri.next()) != null) {
                if (o.getDMOKind() == DMOKind.SEQUENCE) {
                    CloseableIterator si = ((XDMSequence) o).createItemIterator();
                    XDMItem item;
                    while ((item = (XDMItem) si.next()) != null) {
                        System.err.println(item.getStringValue());
                    }
                } else {
                    System.err.println(((XDMItem) o).getStringValue());
                }
            }
        } finally {
            ri.close();
        }
        System.err.println("--- Results end");
    }
}