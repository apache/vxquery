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
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.runtime.functions.type.SequenceTypeMatcher;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/**
 * The rule searches for where the xquery treat function is used and
 * determines if the treat is necessary. The plan is modified if
 * any of these items is not required.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   ASSIGN( $v1 : treat( $v0, \@type_expression ) )
 *   ASSIGN( $v0 : $$constant )
 *   plan__child
 *   
 *   Where $$constant is of \@type_expression.
 *   
 * After 
 * 
 *   plan__parent
 *   ASSIGN( $v1 : $v0 )
 *   ASSIGN( $v0 : $$constant )
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */
// TODO Replace with constant folding rule.
public class RemoveUnusedTreatRule implements IAlgebraicRewriteRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        boolean operatorChanged = false;
        // Check if assign is for treat.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignTreat = (AssignOperator) op;

        // Check to see if the expression is a function and treat.
        ILogicalExpression logicalExpression11 = (ILogicalExpression) assignTreat.getExpressions().get(0).getValue();
        if (logicalExpression11.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionTreat = (AbstractFunctionCallExpression) logicalExpression11;
        if (!functionTreat.getFunctionIdentifier().equals(BuiltinOperators.TREAT.getFunctionIdentifier())) {
            return false;
        }

        // Find the variable id used as the parameter.
        ILogicalExpression treatArg1 = (ILogicalExpression) functionTreat.getArguments().get(0).getValue();
        if (treatArg1.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression variableExpression = (VariableReferenceExpression) treatArg1;
        int variableId = variableExpression.getVariableReference().getId();

        // Get type to check against constant.
        ILogicalExpression treatArg2 = (ILogicalExpression) functionTreat.getArguments().get(1).getValue();
        if (treatArg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        TaggedValuePointable tvp = new TaggedValuePointable();
        getConstantAsPointable((ConstantExpression) treatArg2, tvp);

        IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        tvp.getValue(pTypeCode);
        SequenceType sType = dCtx.lookupSequenceType(pTypeCode.getInteger());

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) assignTreat.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignConstant = (AssignOperator) op2;

        if (variableId == assignConstant.getVariables().get(0).getId()) {
            ILogicalExpression expressionConstant = (ILogicalExpression) assignConstant.getExpressions().get(0)
                    .getValue();
            if (expressionConstant.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                // Check constant against type supplied.
                TaggedValuePointable tvp2 = new TaggedValuePointable();
                getConstantAsPointable((ConstantExpression) expressionConstant, tvp2);

                SequenceTypeMatcher stm = new SequenceTypeMatcher();
                stm.setSequenceType(sType);

                if (stm.sequenceTypeMatch(tvp2)) {
                    assignTreat.getExpressions().get(0).setValue(treatArg1);
                    operatorChanged = true;
                }
            }
        }

        return operatorChanged;
    }

    private void getConstantAsPointable(ConstantExpression typeExpression, TaggedValuePointable tvp) {
        VXQueryConstantValue treatTypeConstant = (VXQueryConstantValue) typeExpression.getValue();
        tvp.set(treatTypeConstant.getValue(), 0, treatTypeConstant.getValue().length);
    }
}
