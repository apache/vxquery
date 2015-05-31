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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public abstract class AbstractCollectionRule implements IAlgebraicRewriteRule {
    final ByteBufferInputStream bbis = new ByteBufferInputStream();
    final DataInputStream di = new DataInputStream(bbis);
    final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

    /**
     * Get the constant value for the collection. Return null for not a collection.
     */
    protected String getCollectionName(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        VXQueryConstantValue constantValue;

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
        ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier())) {
            return null;
        }

        ILogicalExpression logicalExpression2 = (ILogicalExpression) functionCall.getArguments().get(0).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }
        VariableReferenceExpression vre = (VariableReferenceExpression) logicalExpression2;
        Mutable<ILogicalOperator> opRef3 = OperatorToolbox.findProducerOf(opRef, vre.getVariableReference());

        // Get the string assigned to the collection function.
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        if (op3.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign2 = (AssignOperator) op3;

            // Check to see if the expression is a constant expression and type string.
            ILogicalExpression logicalExpression3 = (ILogicalExpression) assign2.getExpressions().get(0).getValue();
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
        String collectionName = null;
        if (tvp.getTag() == ValueTag.XS_STRING_TAG) {
            tvp.getValue(stringp);
            try {
                bbis.setByteBuffer(
                        ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(), stringp.getStartOffset(),
                                stringp.getLength() + stringp.getStartOffset())), 0);
                collectionName = di.readUTF();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return collectionName;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

}
