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
package org.apache.vxquery.xtest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestRunnerFactory {

    private List<ResultReporter> reporters;
    private TestRunner tr;

    public TestRunnerFactory(XTestOptions opts) throws Exception {
        tr = new TestRunner(opts);
        tr.open();
        reporters = new ArrayList<ResultReporter>();
    }

    public void registerReporter(ResultReporter reporter) {
        reporters.add(reporter);
    }

    public Runnable createRunner(final TestCase testCase) {
        return new Runnable() {
            @Override
            public void run() {
                TestCaseResult res = tr.run(testCase);
                for (ResultReporter r : reporters) {
                    try {
                        r.reportResult(res);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    public void registerReporters(Collection<ResultReporter> reporters) {
        this.reporters.addAll(reporters);
    }

    public void close() throws Exception {
        tr.close();
    }
}