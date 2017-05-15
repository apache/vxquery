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

import java.io.File;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class VXQueryIT extends AbstractXQueryTest {

    private static String XQTS_CATALOG = StringUtils.join(new String[] { "test-suites", "xqts", "XQTSCatalog.xml" },
            File.separator);

    public VXQueryIT(TestCase tc) throws Exception {
        super(tc);
    }

    @Parameters(name = "VXQueryIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        JUnitTestCaseFactory jtcf_vxquery = new JUnitTestCaseFactory(getOptions());
        Collection<Object[]> tests = jtcf_vxquery.getList();
        return tests;
    }

    public static XTestOptions getOptions() {
        XTestOptions options = getDefaultTestOptions();
        options.catalog = XQTS_CATALOG;
        options.previousTestResults = StringUtils.join(new String[] { "results", "xqts.txt" }, File.separator);
        options.verbose = true;
        return options;
    }

    @Override
    protected XTestOptions getTestOptions() {
        return getOptions();
    }

}
