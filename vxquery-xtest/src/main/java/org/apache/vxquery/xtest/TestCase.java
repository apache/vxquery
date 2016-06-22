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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

public class TestCase {
    private TestConfiguration tConfig;
    private String folder;
    private String name;
    private Map<QName, File> extVars;
    private String expectedError;
    private List<ExpectedResult> expectedResults;

    public TestCase(TestConfiguration config) {
        this.tConfig = config;
        extVars = new HashMap<>();
        expectedResults = new ArrayList<>();
    }

    public TestConfiguration getConfig() {
        return tConfig;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public String getXQueryDisplayName() {
        return folder + "/" + name;
    }

    public File getXQueryFile() {
        return new File(tConfig.xqueryQueryOffsetPath, folder + "/" + name + tConfig.xqueryFileExtension);
    }

    public File[] getExpectedResultFiles() {
        File[] files = new File[expectedResults.size()];
        for (int i = 0; i < files.length; ++i) {
            files[i] = new File(tConfig.resultOffsetPath, folder + "/" + expectedResults.get(i).fileName);
        }
        return files;
    }

    public void addExternalVariableBinding(QName variable, File file) {
        extVars.put(variable, file);
    }

    public File getExternalVariableBinding(QName varName) {
        return extVars.get(varName);
    }

    public Map<String, File> getSourceFileMap() {
        return Collections.unmodifiableMap(tConfig.sourceFileMap);
    }

    public void setExpectedError(String error) {
        this.expectedError = error;
    }

    public String getExpectedError() {
        return expectedError;
    }

    public void addExpectedResult(ExpectedResult expectedResult) {
        this.expectedResults.add(expectedResult);
    }

    @Override
    public String toString() {
        return getXQueryDisplayName();
    }
}
