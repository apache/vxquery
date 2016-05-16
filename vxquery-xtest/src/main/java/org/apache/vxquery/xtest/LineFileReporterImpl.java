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
import java.io.IOException;
import java.io.PrintWriter;

public class LineFileReporterImpl implements ResultReporter {
    private PrintWriter out;

    public LineFileReporterImpl(File file) throws IOException {
        out = new PrintWriter(file);
    }

    @Override
    public synchronized void reportResult(TestCaseResult result) {
        out.print(result.testCase.getXQueryDisplayName());
        out.print(", ");
        out.println(result.state);
    }

    @Override
    public void close() {
        out.close();
    }
}
