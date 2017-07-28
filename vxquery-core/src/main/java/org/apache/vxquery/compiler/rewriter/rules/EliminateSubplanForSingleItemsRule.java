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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

/**
 * The rule searches for subplans that only have one item per tuple.
 * 
 * <pre>
 * Before
 * 
 *   %PARENT_PLAN
 *   SUBPLAN{
 *     AGGREGATE($v2, sequence(%expression($v1)) )
 *     UNNEST($v1, iterate($v0) )
 *     NESTEDTUPLESOURCE
 *   }
 *   %CHILD_PLAN
 * 
 *   Where |$v0| == 1
 * 
 * After 
 * 
 *   %PARENT_PLAN
 *   ASSIGN($v2, %expression($v0) )
 *   %CHILD_PLAN
 * </pre>
 * 
 * @author prestonc
 */
public class EliminateSubplanForSingleItemsRule implements IAlgebraicRewriteRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;

    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op;

        // AGGREGATE($v2, sequence(%expression($v1)) )
        AbstractLogicalOperator subplanOp1 = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0)
                .getValue();
        if (subplanOp1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) subplanOp1;

        // Check to see if the expression is a function and op:sequence.
        ILogicalExpression logicalExpression1 = (ILogicalExpression) aggregate.getExpressions().get(0).getValue();
        if (logicalExpression1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall1 = (AbstractFunctionCallExpression) logicalExpression1;
        if (!functionCall1.getFunctionIdentifier().equals(BuiltinOperators.SEQUENCE.getFunctionIdentifier())) {
            return false;
        }

        Mutable<ILogicalExpression> lvm1 = ExpressionToolbox.findVariableExpression(aggregate.getExpressions().get(0));
        if (lvm1 == null) {
            return false;
        }
        VariableReferenceExpression vre1 = (VariableReferenceExpression) lvm1.getValue();

        // UNNEST($v1, iterate($v0) )
        AbstractLogicalOperator subplanOp2 = (AbstractLogicalOperator) subplanOp1.getInputs().get(0).getValue();
        if (subplanOp2.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator subplanUnnest = (UnnestOperator) subplanOp2;

        // Check to see if the expression is the iterate operator.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) subplanUnnest.getExpressionRef().getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall2 = (AbstractFunctionCallExpression) logicalExpression2;
        if (!functionCall2.getFunctionIdentifier().equals(BuiltinOperators.ITERATE.getFunctionIdentifier())) {
            return false;
        }

        if (subplanUnnest.getVariable() != vre1.getVariableReference()) {
            return false;
        }
        Mutable<ILogicalExpression> lvm2 = ExpressionToolbox.findVariableExpression(subplanUnnest.getExpressionRef());
        if (lvm2 == null) {
            return false;
        }
        VariableReferenceExpression vre2 = (VariableReferenceExpression) lvm2.getValue();

        // NESTEDTUPLESOURCE
        AbstractLogicalOperator subplanOp3 = (AbstractLogicalOperator) subplanOp2.getInputs().get(0).getValue();
        if (subplanOp3.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        // Ensure input is from a UNNEST operator.
        //TODO: Make it check the cardinality of DataScan
        AbstractLogicalOperator subplanInput = (AbstractLogicalOperator) subplan.getInputs().get(0).getValue();
        if (!(subplanInput.getOperatorTag() == LogicalOperatorTag.ASSIGN
                || subplanInput.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN)) {
            return false;
        }
        ILogicalExpression logicalExpression3 = null;
        if (subplanInput.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign = (AssignOperator) subplanInput;
            if (!assign.getVariables().contains(vre2.getVariableReference())) {
                return false;
            }
            logicalExpression3 = (ILogicalExpression) assign.getExpressions().get(0).getValue();
            if (logicalExpression3.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression functionCall3 = (AbstractFunctionCallExpression) logicalExpression3;
            if (!functionCall3.getFunctionIdentifier().equals(BuiltinOperators.TREAT.getFunctionIdentifier())) {
                return false;
            }

            // Find the treat type.
            ILogicalExpression argType = functionCall3.getArguments().get(ARG_TYPE).getValue();
            if (argType.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return false;
            }
            TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
            ExpressionToolbox.getConstantAsPointable((ConstantExpression) argType, tvp);

            IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
            tvp.getValue(pTypeCode);
            SequenceType sType = dCtx.lookupSequenceType(pTypeCode.getInteger());
            if (sType.getQuantifier() != Quantifier.QUANT_ONE) {
                return false;
            }
        }

        //        if (!assign.getVariables().contains(vre2.getVariableReference())) {
        //            return false;
        //        }

        // Check to see if the expression is the iterate operator.
        //        ILogicalExpression logicalExpression3 = (ILogicalExpression) assign.getExpressions().get(0).getValue();

        // Create replacement assign operator.
        lvm1.setValue(vre2);
        AssignOperator replacementAssign = new AssignOperator(aggregate.getVariables().get(0),
                functionCall1.getArguments().get(0));
        replacementAssign.getInputs().addAll(subplan.getInputs());
        opRef.setValue(replacementAssign);

        return false;
    }

}
