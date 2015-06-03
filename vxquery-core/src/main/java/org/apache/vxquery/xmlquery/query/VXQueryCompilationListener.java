package org.apache.vxquery.xmlquery.query;

import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;
import org.json.JSONException;
import org.kohsuke.args4j.Option;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

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
                System.err.println(jobSpec.toJSON().toString(2));
            } catch (JSONException e) {
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
            LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor(ev);
            PlanPrettyPrinter.printPlan(module.getBody(), sb, v, 0);
        } catch (AlgebricksException e) {
            e.printStackTrace();
        }
        return sb;
    }
    

}
