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
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

public class TestCase {
    private TestConfiguration tConfig;
    private String folder;
    private String name;
    private Map<QName, File> extVars;
    private String expectedError;
    private String outFileName;

    public TestCase(TestConfiguration config) {
        this.tConfig = config;
        extVars = new HashMap<QName, File>();
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

    public File getResultFile() {
        return new File(tConfig.resultOffsetPath, folder + "/" + outFileName);
    }

    public void addExternalVariableBinding(QName variable, File file) {
        extVars.put(variable, file);
    }

    public File getExternalVariableBinding(QName varName) {
        return extVars.get(varName);
    }

    public void setExpectedError(String error) {
        this.expectedError = error;
    }

    public String getExpectedError() {
        return expectedError;
    }

    public void setOutputFileName(String outFile) {
        this.outFileName = outFile;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("TestCase {\n");
        buffer.append("   name = ").append(name).append('\n');
        buffer.append("   vars = {\n");
        for (Map.Entry<QName, File> e : extVars.entrySet()) {
            try {
                buffer.append("      ").append(e.getKey()).append(" = ").append(e.getValue().getCanonicalPath())
                        .append('\n');
            } catch (IOException ex) {
            }
        }
        buffer.append("   }\n");
        buffer.append("}");
        return buffer.toString();
    }
}