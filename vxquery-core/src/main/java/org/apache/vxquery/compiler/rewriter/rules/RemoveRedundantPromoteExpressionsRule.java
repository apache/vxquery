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
package org.apache.vxquery.compiler.rewriter.rules;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/**
 * The rule searches for where the xquery promote function is used. When the
 * expression's return type matches the promote expression type, the promote is
 * removed.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : promote( \@input_expression, \@type_expression ) )
 *   plan__child
 *   
 *   Where promote \@type_expression is the same as the return type of \@input_expression.
 *   
 * After 
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : \@input_expression )
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */

public class RemoveRedundantPromoteExpressionsRule implements IAlgebraicRewriteRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;
        List<Mutable<ILogicalExpression>> expressions = OperatorToolbox.getExpression(opRef);
        for (Mutable<ILogicalExpression> expression : expressions) {
            if (processPromoteExpression(expression)) {
                modified = true;
            }
        }
        return modified;
    }

    private boolean processPromoteExpression(Mutable<ILogicalExpression> search) {
        boolean modified = false;
        Mutable<ILogicalExpression> promoteM = ExpressionToolbox.findFunctionExpression(search,
                BuiltinOperators.PROMOTE.getFunctionIdentifier());
        if (promoteM != null) {
            // Get input function
            AbstractFunctionCallExpression promoteFunction = (AbstractFunctionCallExpression) promoteM.getValue();
            Mutable<ILogicalExpression> argDataM = promoteFunction.getArguments().get(ARG_DATA);
            
            // Find the input return type.
            SequenceType inputSequenceType = null;
            Function function = ExpressionToolbox.getBuiltIn(argDataM);
            if (function == null) {
                return false;
            } else {
                inputSequenceType = function.getSignature().getReturnType();
            }

            // Find the promote type.
            ILogicalExpression argType = promoteFunction.getArguments().get(ARG_TYPE).getValue();
            if (argType.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return false;
            }
            TaggedValuePointable tvp = new TaggedValuePointable();
            ExpressionToolbox.getConstantAsPointable((ConstantExpression) argType, tvp);

            IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
            tvp.getValue(pTypeCode);
            SequenceType sType = dCtx.lookupSequenceType(pTypeCode.getInteger());

            // remove
            if (inputSequenceType != null && inputSequenceType.equals(sType)) {
                promoteM.setValue(argDataM.getValue());
                modified = true;
                processPromoteExpression(argDataM);
            }
        }
        return modified;
    }

}
