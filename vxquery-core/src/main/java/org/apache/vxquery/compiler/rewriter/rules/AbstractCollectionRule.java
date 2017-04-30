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

import java.io.DataInputStream;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

public abstract class AbstractCollectionRule implements IAlgebraicRewriteRule {
    final ByteBufferInputStream bbis = new ByteBufferInputStream();
    final DataInputStream di = new DataInputStream(bbis);
    final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    public static AbstractFunctionCallExpression functionCall;

    /**
     * Get the arguments for the collection and collection-with-tag. Return null for not a collection.
     *
     * @param opRef
     *            Logical operator
     * @param functions
     *            Functions identifiers
     * @return collection name
     */
    protected String[] getFunctionalArguments(Mutable<ILogicalOperator> opRef, Set<FunctionIdentifier> functions) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return null;
        }
        UnnestOperator unnest = (UnnestOperator) op;

        // Check if assign is for fn:Collection.
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return null;
        }
        AssignOperator assign = (AssignOperator) op2;

        // Check to see if the expression is a function and fn:Collection.
        ILogicalExpression logicalExpression = assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        functionCall = (AbstractFunctionCallExpression) logicalExpression;

        if (!functions.contains(functionCall.getFunctionIdentifier())) {
            return null;
        }

        // Get arguments
        int size = functionCall.getArguments().size();
        if (size > 0) {
            String[] args = new String[size];
            for (int i = 0; i < size; i++) {
                args[i] = getArgument(functionCall, opRef, i);
            }
            return args;
        }
        return null;
    }

    private String getArgument(AbstractFunctionCallExpression functionCall, Mutable<ILogicalOperator> opRef, int pos) {
        VXQueryConstantValue constantValue;
        ILogicalExpression logicalExpression2 = functionCall.getArguments().get(pos).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        } else if (logicalExpression2 == null) {
            return null;
        }
        VariableReferenceExpression vre = (VariableReferenceExpression) logicalExpression2;
        Mutable<ILogicalOperator> opRef3 = OperatorToolbox.findProducerOf(opRef, vre.getVariableReference());

        // Get the string assigned to the collection function.
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        if (op3.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign2 = (AssignOperator) op3;

            // Check to see if the expression is a constant expression and type string.
            ILogicalExpression logicalExpression3 = assign2.getExpressions().get(0).getValue();
            if (logicalExpression3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constantExpression = (ConstantExpression) logicalExpression3;
            constantValue = (VXQueryConstantValue) constantExpression.getValue();
            if (constantValue.getType() != SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)) {
                return null;
            }
        } else {
            return null;
        }
        // Constant value is now in a TaggedValuePointable. Convert the value into a java String.
        tvp.set(constantValue.getValue(), 0, constantValue.getValue().length);
        if (tvp.getTag() == ValueTag.XS_STRING_TAG) {
            tvp.getValue(stringp);
            return stringp.toString();
        }
        return null;
    }

    protected boolean setDataSourceScan(IDataSource<String> ids, Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        UnnestOperator unnest = (UnnestOperator) op;
        Mutable<ILogicalOperator> opRef2 = unnest.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        AssignOperator assign = (AssignOperator) op2;

        DataSourceScanOperator opNew = new DataSourceScanOperator(assign.getVariables(), ids);
        opNew.getInputs().addAll(assign.getInputs());
        opRef2.setValue(opNew);

        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

}
