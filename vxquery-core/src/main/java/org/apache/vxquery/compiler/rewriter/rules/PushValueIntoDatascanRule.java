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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
import org.apache.vxquery.metadata.VXQueryIndexingDataSource;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.functions.BuiltinOperators;

import org.apache.commons.lang3.ArrayUtils;

/**
 * The rule searches for two assign operators immediately following a data scan
 * operator.
 *
 * <pre>
 * Before
 *
 *   plan__parent
 *   ASSIGN( $v2 : value( $v1, constant) )
 *   DATASCAN( $source : $v1 )
 *   plan__child
 *
 *   Where $v1 is not used in plan__parent.
 *
 * After
 *
 *   plan__parent
 *   ASSIGN( $v2 : $v1 )
 *   DATASCAN( $source : $v1 )
 *   plan__child
 *
 *   $source is encoded with the value parameters.
 * </pre>
 */

public class PushValueIntoDatascanRule extends AbstractUsedVariablesProcessingRule {
    StaticContext dCtx = null;

    @Override
    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (dCtx == null) {
            VXQueryOptimizationContext vxqueryCtx = (VXQueryOptimizationContext) context;
            dCtx = ((VXQueryMetadataProvider) vxqueryCtx.getMetadataProvider()).getStaticContext();
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op1;

        AbstractLogicalOperator op4 = (AbstractLogicalOperator) assign.getInputs().get(0).getValue();
        if (op4.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        DataSourceScanOperator datascan = (DataSourceScanOperator) op4;

        if (!usedVariables.contains(datascan.getVariables())) {
            VXQueryCollectionDataSource ds = null;
            VXQueryIndexingDataSource ids = null;

            // Find all value functions.
            try {
                ids = (VXQueryIndexingDataSource) datascan.getDataSource();
            } catch (ClassCastException e) {
                ds = (VXQueryCollectionDataSource) datascan.getDataSource();
            }

            if (!updateDataSource(ds, assign.getExpressions().get(0))) {
                return false;
            }
            // Replace assign with noop assign. Keeps variable chain.
            Mutable<ILogicalExpression> varExp = ExpressionToolbox
                    .findVariableExpression(assign.getExpressions().get(0), datascan.getVariables().get(0));
            AssignOperator noOp = new AssignOperator(assign.getVariables().get(0), varExp);
            noOp.getInputs().addAll(assign.getInputs());
            opRef.setValue(noOp);
            return true;
        }
        return false;
    }

    private boolean updateDataSource(VXQueryCollectionDataSource ds, Mutable<ILogicalExpression> expression) {
        boolean added = false;
        ILogicalExpression comparison = null;
        List<Mutable<ILogicalExpression>> finds = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> finds2 = new ArrayList<Mutable<ILogicalExpression>>();
        List<ILogicalExpression> listComparison = new ArrayList<ILogicalExpression>();
        List<ILogicalExpression> listValues = new ArrayList<ILogicalExpression>();
        ExpressionToolbox.findAllFunctionExpressions(expression, BuiltinOperators.VALUE.getFunctionIdentifier(), finds);
        //        ExpressionToolbox.findAllFunctionExpressions(expression,
        //                BuiltinOperators.KEYS_OR_MEMBERS.getFunctionIdentifier(), finds2);
        if (finds.size() > 0) {
            listComparison = ExpressionToolbox.getFullArguments(finds.get(finds.size() - 1));
            comparison = listComparison.get(0);
        }
        for (int i = finds.size(); i > 0; --i) {
            listValues.add(ExpressionToolbox.getFullArguments(finds.get(i-1)).get(0));
            listValues.add(ExpressionToolbox.getFullArguments(finds.get(i-1)).get(1));
        }
//        for (int i = listValues.size(); i > 0; --i) {
//            for (int j = finds2.size(); j > 0; --j) {
//                if (finds.get(i).equals(finds2.get(j))) {
//                    int ena = 0;
//                }
//            }
//        }

        byte[] b = new byte[4];
        b[0] = (byte) 0xff;
        b[1] = (byte) 0x00;
        b[2] = (byte) 0x00;
        b[3] = (byte) 0x00;

        for (int i = finds.size(); i > 0; --i) {
            Byte[] value = ExpressionToolbox.getConstantArgument(finds.get(i - 1), 1);
            List<ILogicalExpression> values = ExpressionToolbox.getFullArguments(finds.get(i - 1));

            ILogicalExpression one = values.get(0);
            if (one.equals(comparison)) {
                ds.addValueSeq(ArrayUtils.toObject(b));
            }

            if (values.size() != 0) {
                ds.addValueSeq(value);
                added = true;
            }
        }

        return added;
    }
}
