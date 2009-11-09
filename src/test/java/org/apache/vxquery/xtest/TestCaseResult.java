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
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Pair;

public class TestCaseResult {
    private static final int DISPLAY_LEN = 1000;

    TestCase testCase;

    Throwable error;

    String result;

    long time;

    String report;

    State state;

    public TestCaseResult(TestCase testCase) {
        this.testCase = testCase;
    }

    public boolean error() {
        return error != null;
    }

    public boolean userError() {
        return error instanceof SystemException;
    }

    public void compare() {
        String eErr = testCase.getExpectedError();
        report = "No result file found";
        state = State.NO_RESULT_FILE;
        if (eErr != null) {
            if (userError()) {
                String aErr = String.valueOf(((SystemException) error).getCode());
                if (eErr.equals(aErr)) {
                    report = "Result matches expected error: " + eErr;
                    state = State.EXPECTED_ERROR_GOT_SAME_ERROR;
                } else {
                    report = "Expected error: " + eErr + ", got error: " + aErr;
                    state = State.EXPECTED_ERROR_GOT_DIFFERENT_ERROR;
                }
            } else if (error()) {
                report = "Expected error: " + eErr + ", got failure: " + error;
                state = State.EXPECTED_ERROR_GOT_FAILURE;
            } else {
                report = "Expected error: " + eErr + ", got result";
                state = State.EXPECTED_ERROR_GOT_RESULT;
            }
        } else {
            if (userError()) {
                String aErr = ((SystemException) error).getCode().toString();
                report = "Expected result, Got error: " + aErr;
                state = State.EXPECTED_RESULT_GOT_ERROR;
            } else if (error()) {
                report = "Expected result, Got failure: " + error;
                state = State.EXPECTED_RESULT_GOT_FAILURE;
            } else {
                File[] resFiles = testCase.getExpectedResultFiles();
                for (int i = 0; i < resFiles.length; ++i) {
                    File resFile = resFiles[i];
                    String expResult = slurpFile(resFile);
                    if (expResult == null) {
                        report = "No result file found";
                        state = State.NO_RESULT_FILE;
                    } else {
                        expResult = expResult.trim();
                        if (result != null) {
                            int idx = result.indexOf("?>");
                            result = result.substring(idx + 1).trim();
                            Pair<Boolean, String> cmp = textCompare(expResult, result);
                            report = cmp.second;
                            state = cmp.first ? State.EXPECTED_RESULT_GOT_SAME_RESULT
                                    : State.EXPECTED_RESULT_GOT_DIFFERENT_RESULT;
                            if (cmp.first) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private Pair<Boolean, String> textCompare(String expRes, String actRes) {
        boolean cmp = expRes.equals(actRes);
        if (testCase.getConfig().options.verbose) {
            System.err.println("Comparing:");
            System.err.println("Expected: ---" + expRes + "---");
            System.err.println("Actual  : ---" + actRes + "---");
            System.err.println("Result  : " + cmp);
        }
        if (cmp) {
            return new Pair<Boolean, String>(Boolean.TRUE, "Got expected result");
        } else {
            return new Pair<Boolean, String>(Boolean.FALSE, "Expected: " + truncate(expRes) + " Got: "
                    + truncate(actRes));
        }
    }

    private String truncate(String str) {
        if (str == null) {
            return "&lt;NULL&gt;";
        }
        if (str.length() > DISPLAY_LEN) {
            str = str.substring(0, DISPLAY_LEN);
            str += "...";
        }
        return xmlEncode(str);
    }

    private String xmlEncode(String str) {
        StringBuilder buffer = new StringBuilder();
        int len = str.length();
        for (int i = 0; i < len; ++i) {
            char ch = str.charAt(i);
            switch (ch) {
                case '<':
                    buffer.append("&lt;");
                    break;

                case '>':
                    buffer.append("&gt;");
                    break;

                case '"':
                    buffer.append("&quot;");
                    break;

                case '\'':
                    buffer.append("&apos;");
                    break;

                case '&':
                    buffer.append("&amp;");
                    break;

                default:
                    buffer.append(ch);
            }
        }
        return buffer.toString();
    }

    private String slurpFile(File f) {
        StringWriter out = new StringWriter();
        try {
            FileReader in = new FileReader(f);
            try {
                char[] buffer = new char[8192];
                int c;
                while ((c = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, c);
                }
            } finally {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        } catch (IOException e) {
            return null;
        }
        return out.toString();
    }

    public enum State {
        EXPECTED_RESULT_GOT_SAME_RESULT("green"),
        EXPECTED_ERROR_GOT_SAME_ERROR("green"),
        EXPECTED_RESULT_GOT_DIFFERENT_RESULT("orange"),
        EXPECTED_RESULT_GOT_ERROR("orange"),
        EXPECTED_ERROR_GOT_DIFFERENT_ERROR("orange"),
        EXPECTED_ERROR_GOT_RESULT("orange"),
        EXPECTED_RESULT_GOT_FAILURE("red"),
        EXPECTED_ERROR_GOT_FAILURE("red"),
        NO_RESULT_FILE("red");

        private String color;

        private State(String color) {
            this.color = color;
        }

        public String getColor() {
            return color;
        }
    }
}