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
package org.apache.vxquery.xtest;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class VXQueryTest {
    private TestCase tc;
    private TestRunner tr;

    private static String VXQUERY_CATALOG = StringUtils.join(new String[] { "src", "test", "resources",
            "VXQueryCatalog.xml" }, File.separator);
    private static String XQTS_CATALOG = StringUtils.join(new String[] { "test-suites", "xqts", "XQTSCatalog.xml" },
            File.separator);

    private static boolean includeXqtsTests() {
        return new File(XQTS_CATALOG).isFile();
    }

    private static XTestOptions getVXQueryOptions() {
        XTestOptions opts = new XTestOptions();
        opts.catalog = VXQUERY_CATALOG;
        opts.verbose = false;
        opts.threads = 1;
        return opts;
    }

    private static XTestOptions getPreviousTestOptions() {
        XTestOptions opts = new XTestOptions();
        opts.catalog = XQTS_CATALOG;
        opts.previousTestResults = StringUtils.join(new String[] { "results", "xqts.txt" }, File.separator);
        opts.verbose = false;
        opts.threads = 1;
        return opts;
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        JUnitTestCaseFactory jtcf_vxquery = new JUnitTestCaseFactory(getVXQueryOptions());
        Collection<Object[]> tests = jtcf_vxquery.getList();
        if (includeXqtsTests()) {
            JUnitTestCaseFactory jtcf_previous = new JUnitTestCaseFactory(getPreviousTestOptions());
            // TODO Maven fails to run when including XQTS. (Error to many open files.)
            //            tests.addAll(jtcf_previous.getList());
        }
        return tests;
    }

    public VXQueryTest(TestCase tc) throws Exception {
        this.tc = tc;
        tr = new TestRunner(getVXQueryOptions());
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
                fail(result.state + " (" + result.time + " ms): " + result.testCase.getXQueryDisplayName());
                break;
            case EXPECTED_ERROR_GOT_SAME_ERROR:
            case EXPECTED_RESULT_GOT_SAME_RESULT:
                break;
            case NO_RESULT_FILE:
                break;
        }
    }

    @After
    public void afterTest() throws Exception {
        tr.close();
    }

}
