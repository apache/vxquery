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