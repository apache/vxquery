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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
import org.apache.vxquery.types.AnyItemType;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class CollectionRewriteRule implements IAlgebraicRewriteRule {
    final ByteBufferInputStream bbis = new ByteBufferInputStream();
    final DataInputStream di = new DataInputStream(bbis);
    final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();

    /**
     * Find the default query plan created for collection and updated it to use parallelization.
     * The following is an example of of the operators we are looking with a constant for the collection name.
     * Search pattern: unnest <- assign [function-call: collection] <- assign [constant: string]
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;

        // Check if assign is for fn:Collection.
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op2;

        // Check to see if the expression is a function and fn:Collection.
        ILogicalExpression logicalExpression = (ILogicalExpression) assign.getExpressions().get(0).getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        if (!functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier())) {
            return false;
        }

        // Get the string assigned to the collection function.
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign2 = (AssignOperator) op3;

        // Check to see if the expression is a constant expression and type string.
        ILogicalExpression logicalExpression2 = (ILogicalExpression) assign2.getExpressions().get(0).getValue();
        if (logicalExpression2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression constantExpression = (ConstantExpression) logicalExpression2;
        VXQueryConstantValue constantValue = (VXQueryConstantValue) constantExpression.getValue();
        if (constantValue.getType() != SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)) {
            return false;
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

        // Build the new operator and update the query plan.
        List<Object> types = new ArrayList<Object>();
        types.add(SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR));
        VXQueryCollectionDataSource ds = new VXQueryCollectionDataSource(collectionName, types.toArray());
        DataSourceScanOperator opNew = new DataSourceScanOperator(unnest.getVariables(), ds);
        opNew.getInputs().addAll(assign.getInputs());
        opRef.setValue(opNew);
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
