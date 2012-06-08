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

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;

public class TestRunnerFactory {
    private List<ResultReporter> reporters;
    private XTestOptions opts;

    public TestRunnerFactory(XTestOptions opts) {
        reporters = new ArrayList<ResultReporter>();
        this.opts = opts;
    }

    public void registerReporter(ResultReporter reporter) {
        reporters.add(reporter);
    }

    public Runnable createRunner(final TestCase testCase) {
        return new Runnable() {
            @Override
            public void run() {
                TestCaseResult res = new TestCaseResult(testCase);
                if (opts.verbose) {
                    System.err.println("Starting " + testCase.getXQueryDisplayName());
                }
                long start = System.currentTimeMillis();
                try {
                    XMLQueryCompiler compiler = new XMLQueryCompiler(null);
                    FileReader in = new FileReader(testCase.getXQueryFile());
                    CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(
                            RootStaticContextImpl.INSTANCE));
                    compiler.compile(testCase.getXQueryDisplayName(), in, ccb, opts.optimizationLevel);
                } catch (Throwable e) {
                    res.error = e;
                } finally {
                    try {
                        res.compare();
                    } catch (Exception e) {
                        System.err.println("Framework error");
                        e.printStackTrace();
                    }
                    long end = System.currentTimeMillis();
                    res.time = end - start;
                    for (ResultReporter r : reporters) {
                        try {
                            r.reportResult(res);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
    }

    public void registerReporters(Collection<ResultReporter> reporters) {
        this.reporters.addAll(reporters);
    }
}