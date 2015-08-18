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
public class XMarkTest extends AbstractXQueryTest {

    private static String XMARK_CATALOG = StringUtils.join(new String[] { "src", "test", "resources",
            "XMarkCatalog.xml" }, File.separator);

    public XMarkTest(TestCase tc) throws Exception {
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
        options.catalog = XMARK_CATALOG;
        options.frameSize = (int) Math.pow(2, 23);
        return options;
    }

    @Override
    protected XTestOptions getTestOptions() {
        return getOptions();
    }

}
