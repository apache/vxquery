package org.apache.vxquery.compiler.rewriter.rules;

import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExpressionPrinter;
import org.apache.vxquery.compiler.rewriter.framework.RewriteRule;
import org.apache.vxquery.xmlquery.query.XMLQueryOptimizer;

public class LoggingRewriteRule implements RewriteRule {
    private final RewriteRule delegate;

    public LoggingRewriteRule(RewriteRule delagate) {
        this.delegate = delagate;
    }

    @Override
    public int getMinOptimizationLevel() {
        return delegate.getMinOptimizationLevel();
    }

    @Override
    public boolean rewritePre(ExpressionHandle exprHandle) {
        boolean result = delegate.rewritePre(exprHandle);
        if (result) {
            XMLQueryOptimizer.LOGGER.fine(delegate.getClass().getName() + ".rewritePre() SUCCEEDED");
            XMLQueryOptimizer.LOGGER.fine(ExpressionPrinter.prettyPrint(exprHandle.get()));
        }
        return result;
    }

    @Override
    public boolean rewritePost(ExpressionHandle exprHandle) {
        boolean result = delegate.rewritePost(exprHandle);
        if (result) {
            XMLQueryOptimizer.LOGGER.fine(delegate.getClass().getName() + ".rewritePost() SUCCEEDED");
            XMLQueryOptimizer.LOGGER.fine(ExpressionPrinter.prettyPrint(exprHandle.get()));
        }
        return result;
    }
}