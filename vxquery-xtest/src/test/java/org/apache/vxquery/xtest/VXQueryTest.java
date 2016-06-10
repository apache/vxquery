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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class VXQueryTest extends AbstractXQueryTest {
    private static MiniDFS dfs;
    private final static String TMP = "target/tmp";

    private static String VXQUERY_CATALOG = StringUtils
            .join(new String[] { "src", "test", "resources", "VXQueryCatalog.xml" }, File.separator);

    public VXQueryTest(TestCase tc) throws Exception {
        super(tc);
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        JUnitTestCaseFactory jtcf_vxquery = new JUnitTestCaseFactory(getOptions());
        Collection<Object[]> tests = jtcf_vxquery.getList();
        return tests;
    }

    public static XTestOptions getOptions() {
        XTestOptions options = getDefaultTestOptions();
        options.catalog = VXQUERY_CATALOG;
        return options;
    }

    @Override
    protected XTestOptions getTestOptions() {
        return getOptions();
    }

    @BeforeClass
    public static void setup() throws IOException {
        File tmp = new File(TMP);
        if (tmp.exists()) {
            FileUtils.deleteDirectory(tmp);
        }
        new File(TMP.concat("/indexFolder")).mkdirs();
        String HDFSFolder = TMP.concat("/hdfsFolder");
        new File(HDFSFolder).mkdirs();
        dfs = new MiniDFS();
        try {
            dfs.startHDFS(HDFSFolder);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    @AfterClass
    public static void shutdown() throws IOException {
        File tmp = new File(TMP);
        if (tmp.exists()) {
            FileUtils.deleteDirectory(tmp);
        }
        dfs.shutdownHDFS();
    }

}
