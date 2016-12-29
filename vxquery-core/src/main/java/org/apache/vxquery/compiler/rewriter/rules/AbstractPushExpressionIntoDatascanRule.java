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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.metadata.IVXQueryDataSource;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;

public abstract class AbstractPushExpressionIntoDatascanRule extends AbstractUsedVariablesProcessingRule {
    StaticContext dCtx = null;
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;

    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean unnestOp = false;
        boolean assignOp = false;

        UnnestOperator unnest = null;
        AssignOperator assign = null;
        AbstractLogicalOperator op2 = null;

        if (dCtx == null) {
            VXQueryOptimizationContext vxqueryCtx = (VXQueryOptimizationContext) context;
            dCtx = ((VXQueryMetadataProvider) vxqueryCtx.getMetadataProvider()).getStaticContext();
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (!(op1.getOperatorTag() == getOperator())) {
            return false;
        }
        if (op1.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            unnest = (UnnestOperator) op1;
            unnestOp = true;
            op2 = (AbstractLogicalOperator) unnest.getInputs().get(0).getValue();
        } else {
            assign = (AssignOperator) op1;
            assignOp = true;
            op2 = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        }

        if (op2.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        DataSourceScanOperator datascan = (DataSourceScanOperator) op2;

        if (!usedVariables.contains(datascan.getVariables())) {

            Mutable<ILogicalExpression> expressionRef = null;
            if (unnestOp) {
                expressionRef = unnest.getExpressionRef();
            } else if (assignOp) {
                expressionRef = assign.getExpressions().get(0);
            }
            if (!(updateDataSource((IVXQueryDataSource) datascan.getDataSource(), expressionRef))) {
                return false;
            }
            if (unnestOp) {
                Mutable<ILogicalExpression> varExp = ExpressionToolbox.findVariableExpression(expressionRef,
                        datascan.getVariables().get(0));
                AssignOperator noOp = new AssignOperator(unnest.getVariable(), varExp);
                noOp.getInputs().addAll(unnest.getInputs());
                opRef.setValue(noOp);
            } else if (assignOp) {
                Mutable<ILogicalExpression> varExp = ExpressionToolbox
                        .findVariableExpression(assign.getExpressions().get(0), datascan.getVariables().get(0));
                AssignOperator noOp = new AssignOperator(assign.getVariables().get(0), varExp);
                noOp.getInputs().addAll(assign.getInputs());
                opRef.setValue(noOp);
            }

            return true;
        }
        return false;

    }

    abstract boolean updateDataSource(IVXQueryDataSource datasource, Mutable<ILogicalExpression> expression);

    abstract LogicalOperatorTag getOperator();

}
