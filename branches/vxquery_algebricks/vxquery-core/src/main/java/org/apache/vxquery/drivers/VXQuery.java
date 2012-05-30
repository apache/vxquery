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
package org.apache.vxquery.drivers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.vxquery.api.InternalAPI;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.dtm.DTMDatamodelStaticInterfaceImpl;
import org.apache.vxquery.v0datamodel.serialization.XMLSerializer;
import org.apache.vxquery.v0runtime.base.OpenableCloseableIterator;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.PrologVariable;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class VXQuery {
    public static void main(String[] args) throws Exception {
        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        if (opts.arguments.isEmpty()) {
            parser.printUsage(System.err);
            return;
        }
        InternalAPI iapi = new InternalAPI(new DTMDatamodelStaticInterfaceImpl());
        for (String query : opts.arguments) {
            String qStr = slurp(query);
            if (opts.showQuery) {
                System.err.println(qStr);
            }
            ModuleNode ast = iapi.parse(query, new StringReader(qStr));
            if (opts.showAST) {
                System.err.println(new XStream(new DomDriver()).toXML(ast));
            }
            XQueryCompilationListener listener = new XQueryCompilationListener() {
                @Override
                public void notifyCodegenResult(Module module) {
                    /*
                    if (opts.showRI) {
                        System.err.println(new XStream(new DomDriver()).toXML(module.getBodyRuntimePlan()
                                .getRuntimeIterator()));
                    }
                    */
                }

                @Override
                public void notifyTranslationResult(Module module) {
                    if (opts.showTET) {
                        try {
                            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor();
                            StringBuilder buffer = new StringBuilder();
                            PlanPrettyPrinter.printPlan(module.getBody(), buffer, v, 0);
                            System.err.println(buffer.toString());
                        } catch (AlgebricksException e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void notifyTypecheckResult(Module module) {
                }

                @Override
                public void notifyOptimizedResult(Module module) {
                    if (opts.showOET) {
                        try {
                            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor();
                            StringBuilder buffer = new StringBuilder();
                            PlanPrettyPrinter.printPlan(module.getBody(), buffer, v, 0);
                            System.err.println(buffer.toString());
                        } catch (AlgebricksException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
            Module module = iapi.compile(listener, ast, opts.optimizationLevel);
            if (opts.compileOnly) {
                continue;
            }
            for (PrologVariable pVar : module.getPrologVariables()) {
                String fName = opts.bindings.get(pVar.getVariable().getName().getLocalPart());
                if (fName != null) {
                    File f = new File(fName);
                    System.err.println("Binding: " + pVar.getVariable().getName() + " to " + f.getAbsolutePath());
                    iapi.bindExternalVariable(pVar.getVariable(), f);
                }
            }
            OpenableCloseableIterator ri = iapi.execute(module);
            for (int i = 0; i < opts.repeatExec; ++i) {
                long start = System.currentTimeMillis();
                ri.open();
                System.err.println("--- Results begin");
                XDMItem o;
                PrintWriter out = new PrintWriter(System.out, true);
                XMLSerializer s = new XMLSerializer(out, false);
                s.open();
                try {
                    while ((o = (XDMItem) ri.next()) != null) {
                        s.item(o);
                    }
                } finally {
                    s.close();
                    out.flush();
                    ri.close();
                }
                System.err.println("--- Results end");
                long end = System.currentTimeMillis();
                if (opts.timing) {
                    System.err.println("Run time: " + (end - start) + " ms");
                }
            }
        }
    }

    private static String slurp(String query) throws IOException {
        return FileUtils.readFileToString(new File(query));
    }

    private static class CmdLineOptions {
        @Option(name = "-O", usage = "Optimization Level. Default: Full Optimization")
        private int optimizationLevel = Integer.MAX_VALUE;

        @Option(name = "-showquery", usage = "Show query string")
        private boolean showQuery;

        @Option(name = "-showast", usage = "Show abstract syntax tree")
        private boolean showAST;

        @Option(name = "-showtet", usage = "Show translated expression tree")
        private boolean showTET;

        @Option(name = "-showoet", usage = "Show optimized expression tree")
        private boolean showOET;

        @Option(name = "-showri", usage = "Show Runtime plan")
        private boolean showRI;

        @Option(name = "-compileonly", usage = "Compile the query and stop")
        private boolean compileOnly;

        @Option(name = "-repeatexec", usage = "Number of times to repeat execution")
        private int repeatExec = 1;

        @Option(name = "-timing", usage = "Produce timing information")
        private boolean timing;

        @Option(name = "-x", usage = "Bind an external variable")
        private Map<String, String> bindings = new HashMap<String, String>();

        @Argument
        private List<String> arguments = new ArrayList<String>();
    }
}