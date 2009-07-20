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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import org.apache.vxquery.xtest.TestCaseResult.State;

public class Reporter extends AbstractHandler {
    private List<TestCaseResult> results;

    private int count;

    private int userErrors;

    private int internalErrors;

    private Map<Class<?>, Integer> exDistribution;

    private Map<TestCaseResult.State, Integer> stDistribution;

    private long startTime;

    private long endTime;

    public Reporter() {
        results = new ArrayList<TestCaseResult>();
        count = 0;
        userErrors = 0;
        internalErrors = 0;
        startTime = -1;
        exDistribution = new HashMap<Class<?>, Integer>();
        stDistribution = new HashMap<TestCaseResult.State, Integer>();
    }

    @Override
    public synchronized void handle(String target, HttpServletRequest request, HttpServletResponse response,
            int dispatch) throws IOException, ServletException {
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter out = response.getWriter();
        out.println("<html><body>");
        out.println("<table>");
        out.println("<tr><td>Test Count</td><td>");
        out.println(count);
        out.println("</td></tr>");
        out.println("<tr><td>Test Errors</td><td>");
        out.println(userErrors);
        out.println("</td></tr>");
        out.println("<tr><td>Test Failures</td><td>");
        out.println(internalErrors);
        out.println("</td></tr>");
        out.println("<tr><td>Total time</td><td>");
        out.println(endTime - startTime);
        out.println("</td></tr>");
        out.println("</table>");
        out.println("<table border=\"1\">");
        for (Map.Entry<Class<?>, Integer> e : exDistribution.entrySet()) {
            out.println("<tr><td>");
            out.println(e.getKey().getName());
            out.println("</td><td>");
            out.println(e.getValue());
            out.println("</td></tr>");
        }
        out.println("</table>");
        out.println("<table border=\"1\">");
        for (Map.Entry<TestCaseResult.State, Integer> e : stDistribution.entrySet()) {
            State key = e.getKey();
            out.print("<tr style=\"background: ");
            out.println(key.getColor());
            out.println(";\"><td>");
            out.println(key);
            out.println("</td><td>");
            out.println(e.getValue());
            out.println("</td></tr>");
        }
        out.println("</table>");
        out.println("<table>");
        int len = results.size();
        for (int i = 0; i < len; ++i) {
            TestCaseResult res = results.get(i);
            out.print("<tr style=\"background: ");
            out.println(res.state.getColor());
            out.println(";\"><td>");
            out.print(i + 1);
            out.print("</td><td>");
            out.print(res.testCase.getXQueryDisplayName());
            out.print("</td><td>");
            out.print(res.time);
            out.print("</td><td>");
            out.print(res.report);
            out.println("</td></tr>");
        }
        out.println("</table></body></html>");
        out.flush();
        ((Request) request).setHandled(true);
    }

    synchronized void reportResult(TestCaseResult result) {
        results.add(result);
        endTime = System.currentTimeMillis();
        if (startTime < 0) {
            startTime = endTime;
        }
        if (result.error()) {
            if (result.userError()) {
                ++userErrors;
            } else {
                ++internalErrors;
                Integer count = exDistribution.get(result.error.getClass());
                if (count == null) {
                    count = 0;
                }
                count++;
                exDistribution.put(result.error.getClass(), count);
            }
        }
        Integer stCount = stDistribution.get(result.state);
        if (stCount == null) {
            stCount = 0;
        }
        stCount++;
        stDistribution.put(result.state, stCount);
        ++count;
    }
}