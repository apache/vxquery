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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class VXQueryTest {

    private static final String PATH_QUERIES = StringUtils.join(new String[] { "Queries", "XQuery" + File.separator },
            File.separator);
    private static final String PATH_RESULTS = "ExpectedTestResults" + File.separator;
    private static final String PATH_TESTS = "cat" + File.separator;
    private static final String PATH_BASE = StringUtils.join(
            new String[] { "src", "test", "resources" + File.separator }, File.separator);

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        return null;
    }

    public VXQueryTest() {
    }

    @Test
    public void test() throws Exception {
    }
}