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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.metadata.IVXQueryDataSource;
import org.apache.vxquery.metadata.VXQueryIndexingDataSource;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.types.ElementType;

public class PushIndexingValueIntoDatascanRule extends AbstractUsedVariablesProcessingRule {
    StaticContext dCtx = null;

    @Override
    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        AbstractLogicalOperator op2 = null;
        AbstractLogicalOperator op3 = null;

        if (dCtx == null) {
            VXQueryOptimizationContext vxqueryCtx = (VXQueryOptimizationContext) context;
            dCtx = ((VXQueryMetadataProvider) vxqueryCtx.getMetadataProvider()).getStaticContext();
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op1;

        op2 = (AbstractLogicalOperator) select.getInputs().get(0).getValue();

        if (op2.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return false;
        }
        ExchangeOperator exchange = (ExchangeOperator) op2;
        op3 = (AbstractLogicalOperator) exchange.getInputs().get(0).getValue();

        if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }

        DataSourceScanOperator datascan = (DataSourceScanOperator) op3;

        if (!usedVariables.contains(datascan.getVariables())) {

            Mutable<ILogicalExpression> expressionRef = select.getCondition();
            if (!(updateDataSource((IVXQueryDataSource) datascan.getDataSource(), expressionRef))) {
                return false;
            }
            context.addToDontApplySet(this, opRef.getValue());
            return true;
        }

        return false;

    }

    private boolean updateDataSource(IVXQueryDataSource dataSource, Mutable<ILogicalExpression> expression) {
        if (!dataSource.usingIndex()) {
            return false;
        }
        VXQueryIndexingDataSource ids = (VXQueryIndexingDataSource) dataSource;
        boolean added = false;
        List<Mutable<ILogicalExpression>> children = new ArrayList<Mutable<ILogicalExpression>>();
        ILogicalExpression selCond = expression.getValue();

        if (selCond.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) selCond;
        ILogicalExpression arguments = afce.getArguments().get(0).getValue();
        if (arguments.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression valueEq = (AbstractFunctionCallExpression) arguments;

        if (!valueEq.getFunctionIdentifier().equals(BuiltinOperators.VALUE_EQ.getFunctionIdentifier())) {
            return false;
        }
        ExpressionToolbox.findAllFunctionExpressions(valueEq.getArguments().get(0),
                BuiltinOperators.CHILD.getFunctionIdentifier(), children);
        for (int i = children.size(); i > 0; --i) {
            int typeId = ExpressionToolbox.getTypeExpressionTypeArgument(children.get(i - 1));
            if (typeId > 0) {
                ElementType it = (ElementType) dCtx.lookupSequenceType(typeId).getItemType();
                ElementType et = ElementType.ANYELEMENT;

                if (it.getContentType().equals(et.getContentType())) {
                    ids.addChildSeq(typeId);
                }
            }
        }
        Byte[] index = convertConstantToInteger(valueEq.getArguments().get(1));

        ids.addIndexSeq(index);
        added = true;
        return added;
    }

    public Byte[] convertConstantToInteger(Mutable<ILogicalExpression> finds) {
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        ExpressionToolbox.getConstantAsPointable((ConstantExpression) finds.getValue(), tvp);

        return ArrayUtils.toObject(tvp.getByteArray());
    }

}
