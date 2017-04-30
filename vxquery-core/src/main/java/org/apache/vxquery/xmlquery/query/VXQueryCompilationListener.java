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
package org.apache.vxquery.xmlquery.query;

import java.io.IOException;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.xmlquery.ast.ModuleNode;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class VXQueryCompilationListener implements XQueryCompilationListener {
    boolean showTET, showRP, showOET, showAST;

    public VXQueryCompilationListener(boolean showAST, boolean showTET, boolean showOET, boolean showRP) {
        this.showTET = showTET;
        this.showRP = showRP;
        this.showOET = showOET;
        this.showAST = showAST;
    }

    /**
     * Outputs the query inputs, outputs and user constraints for each module as result of code generation.
     *
     * @param module
     */
    public void notifyCodegenResult(Module module) {
        if (showRP) {
            JobSpecification jobSpec = module.getHyracksJobSpecification();
            try {
                System.err.println("***Runtime Plan: ");
                System.err.println(jobSpec.toJSON().toString());
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(jobSpec.toString());
            }
        }
    }

    /**
     * Outputs the syntax translation tree for the module in the format: "-- logical operator(if exists) | execution mode |"
     * where execution mode can be one of: UNPARTITIONED,PARTITIONED,LOCAL
     *
     * @param module
     */
    @Override
    public void notifyTranslationResult(Module module) {
        if (showTET) {
            System.err.println("***Translated Expression Tree: ");
            System.err.println(appendPrettyPlan(new StringBuilder(), module).toString());
        }
    }

    @Override
    public void notifyTypecheckResult(Module module) {
    }

    /**
     * Outputs the optimized expression tree for the module in the format:
     * "-- logical operator(if exists) | execution mode |" where execution mode can be one of: UNPARTITIONED,PARTITIONED,LOCAL
     *
     * @param module
     */
    @Override
    public void notifyOptimizedResult(Module module) {
        if (showOET) {
            System.err.println("***Optimized Expression Tree: ");
            System.err.println(appendPrettyPlan(new StringBuilder(), module).toString());
        }
    }

    /**
     * Outputs the abstract syntax tree obtained from parsing by serializing the DomDriver object to a pretty-printed XML
     * String.
     *
     * @param moduleNode
     */
    @Override
    public void notifyParseResult(ModuleNode moduleNode) {
        if (showAST) {
            System.err.println("***Abstract Syntax Tree: ");
            System.err.println(new XStream(new DomDriver()).toXML(moduleNode));
        }
    }

    private StringBuilder appendPrettyPlan(StringBuilder sb, Module module) {
        try {
            ILogicalExpressionVisitor<String, Integer> ev = new VXQueryLogicalExpressionPrettyPrintVisitor(
                    module.getModuleContext());
            AlgebricksAppendable buffer = new AlgebricksAppendable();
            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor(buffer, ev);
            PlanPrettyPrinter.printPlan(module.getBody(), v, 0);
            sb.append(buffer.toString());
        } catch (AlgebricksException e) {
            e.printStackTrace();
        }
        return sb;
    }
}
