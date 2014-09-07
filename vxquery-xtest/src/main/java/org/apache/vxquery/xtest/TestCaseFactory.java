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

import java.util.concurrent.ExecutorService;

public class TestCaseFactory extends AbstractTestCaseFactory {

    public TestRunnerFactory trf;
    public ExecutorService eSvc;

    public TestCaseFactory(TestRunnerFactory trf, ExecutorService eSvc, XTestOptions opts) {
        super(opts);
        this.trf = trf;
        this.eSvc = eSvc;
    }

    protected void submit(TestCase tc) {
        boolean toSubmit = include == null || include.matcher(tc.getXQueryDisplayName()).find();
        toSubmit = toSubmit && (exclude == null || !exclude.matcher(tc.getXQueryDisplayName()).find());
        if (toSubmit) {
            if (opts.verbose) {
                System.err.println(tc);
            }
            eSvc.submit(trf.createRunner(tc));
            ++count;
        }
    }

}