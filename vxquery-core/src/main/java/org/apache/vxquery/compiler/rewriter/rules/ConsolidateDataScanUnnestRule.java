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
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/**
 * The rule searches for an unnest operator immediately following a data scan
 * operator.
 * 
 * <pre>
 * Before 
 * 
 *   plan__parent
 *   UNNEST( $v2 : child( $v1 ) )
 *   DATASCAN( $source : $v1 )
 *   plan__child
 *   
 *   Where $v1 is not used in plan__parent.
 *   
 * After
 * 
 *   plan__parent
 *   DATASCAN( $source : $v1 )
 *   plan__child
 *   
 *   $source is encoded with the child parameters.
 * </pre>
 * 
 * @author prestonc
 */
public class ConsolidateDataScanUnnestRule extends AbstractUsedVariablesProcessingRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;

    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getInputs().isEmpty()) {
            return false;
        }

        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op1;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        DataSourceScanOperator datascan = (DataSourceScanOperator) op2;

        if (!usedVariables.contains(datascan.getVariables())) {
            // Check to see if the unnest expression is a child function.
            ILogicalExpression logicalExpression = (ILogicalExpression) unnest.getExpressionRef().getValue();
            if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
            Function functionInfo = (Function) functionCall.getFunctionInfo();
            if (!functionInfo.getFunctionIdentifier().equals(BuiltinOperators.CHILD.getFunctionIdentifier())) {
                return false;
            }

            // Find all child functions.
            VXQueryCollectionDataSource ds = (VXQueryCollectionDataSource) datascan.getDataSource();
            updateDataSource(ds, unnest.getExpressionRef());

            // Replace unnest with noop assign. Keeps variable chain.
            Mutable<ILogicalExpression> varExp = ExpressionToolbox.findVariableExpression(unnest.getExpressionRef(),
                    datascan.getVariables().get(0));
            AssignOperator noOp = new AssignOperator(unnest.getVariable(), varExp);
            noOp.getInputs().addAll(unnest.getInputs());
            op0.getInputs().clear();
            op0.getInputs().add(new MutableObject<ILogicalOperator>(noOp));
            return true;
        }
        return false;
    }

    /**
     * In reverse add them to the data source.
     * 
     * @param ds
     * @param expression
     */
    private void updateDataSource(VXQueryCollectionDataSource ds, Mutable<ILogicalExpression> expression) {
        ILogicalExpression logicalExpression = (ILogicalExpression) expression.getValue();
        if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) logicalExpression;
        Function functionInfo = (Function) functionCall.getFunctionInfo();
        if (!functionInfo.getFunctionIdentifier().equals(BuiltinOperators.CHILD.getFunctionIdentifier())) {
            return;
        }
        // Traverse down child function nesting.
        updateDataSource(ds, functionCall.getArguments().get(ARG_DATA));

        // Add the child type parameter to data source.
        ILogicalExpression argType = functionCall.getArguments().get(ARG_TYPE).getValue();
        if (argType.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return;
        }
        TaggedValuePointable tvp = new TaggedValuePointable();
        ExpressionToolbox.getConstantAsPointable((ConstantExpression) argType, tvp);

        IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        tvp.getValue(pTypeCode);
        ds.addChildSeq(pTypeCode.getInteger());
    }
}
