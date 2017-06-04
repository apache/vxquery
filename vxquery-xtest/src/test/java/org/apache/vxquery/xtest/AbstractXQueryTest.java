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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class AbstractXQueryTest {
    private TestCase tc;
    private TestRunner tr;
    private static MiniDFS dfs;
    private final static String TMP = "target/tmp";

    protected abstract XTestOptions getTestOptions();

    protected static XTestOptions getDefaultTestOptions() {
        XTestOptions opts = new XTestOptions();
        opts.verbose = false;
        opts.threads = 1;
        opts.showQuery = true;
        opts.showResult = true;
        opts.hdfsConf = "src/test/resources/hadoop/conf";
        return opts;
    }

    public AbstractXQueryTest(TestCase tc) throws Exception {
        this.tc = tc;
        tr = new TestRunner(getTestOptions());
    }

    @Before
    public void beforeTest() throws Exception {
        tr.open();
    }

    @Test
    public void test() throws Exception {
        TestCaseResult result = tr.run(tc);
        switch (result.state) {
            case EXPECTED_ERROR_GOT_DIFFERENT_ERROR:
            case EXPECTED_ERROR_GOT_FAILURE:
            case EXPECTED_ERROR_GOT_RESULT:
            case EXPECTED_RESULT_GOT_DIFFERENT_RESULT:
            case EXPECTED_RESULT_GOT_ERROR:
            case EXPECTED_RESULT_GOT_FAILURE:
                fail(result.state + " (" + result.time + " ms): " + result.testCase.getXQueryDisplayName() + " "
                        + result.error);
                break;
            case EXPECTED_ERROR_GOT_SAME_ERROR:
            case EXPECTED_RESULT_GOT_SAME_RESULT:
                break;
            case NO_RESULT_FILE:
                fail(result.state + " (" + result.time + " ms): " + result.testCase.getXQueryDisplayName());
                break;
        }
    }

    @After
    public void afterTest() throws Exception {
        tr.close();
    }

    @BeforeClass
    public static void setup() throws IOException {
        TestClusterUtil.startCluster(getDefaultTestOptions(), TestClusterUtil.localClusterUtil);
        setupFS();
    }

    public static void setupFS() throws IOException {
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
            throw new IOException(e);
        }
    }

    @AfterClass
    public static void shutdown() throws IOException {
        removeFS();
        TestClusterUtil.stopCluster(TestClusterUtil.localClusterUtil);
    }

    public static void removeFS() throws IOException {
        dfs.shutdownHDFS();
        File tmp = new File(TMP);
        if (tmp.exists()) {
            FileUtils.deleteDirectory(tmp);
        }
    }

}
