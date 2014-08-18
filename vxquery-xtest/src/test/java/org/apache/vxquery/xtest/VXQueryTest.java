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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class VXQueryTest {
    private TestCase tc;
    private TestRunner tr;

    private static String CATALOG = "VXQueryCatalog.xml";

    private static XTestOptions getOptions() {
        XTestOptions opts = new XTestOptions();
        opts.catalog = StringUtils.join(new String[] { "src", "test", "resources", CATALOG }, File.separator);
        opts.verbose = true;
        opts.threads = 1;
        return opts;
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        JUnitTestCaseFactory jtcf = new JUnitTestCaseFactory(getOptions());
        return jtcf.getList();
    }

    public VXQueryTest(TestCase tc) throws Exception {
        this.tc = tc;
        tr = new TestRunner(getOptions());
    }

    @Test
    public void test() throws Exception {
        tr.open();
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
        tr.close();
    }
}
