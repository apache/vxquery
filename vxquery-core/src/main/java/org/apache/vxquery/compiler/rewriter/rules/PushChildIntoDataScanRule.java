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
import java.util.ListIterator;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
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
public class PushChildIntoDataScanRule extends AbstractUsedVariablesProcessingRule {
    final StaticContextImpl dCtx = new StaticContextImpl(RootStaticContextImpl.INSTANCE);
    final int ARG_DATA = 0;
    final int ARG_TYPE = 1;

    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();;
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
            // Find all child functions.
            VXQueryCollectionDataSource ds = (VXQueryCollectionDataSource) datascan.getDataSource();
            if (!updateDataSource(ds, unnest.getExpressionRef())) {
                return false;
            }

            // Replace unnest with noop assign. Keeps variable chain.
            Mutable<ILogicalExpression> varExp = ExpressionToolbox.findVariableExpression(unnest.getExpressionRef(),
                    datascan.getVariables().get(0));
            AssignOperator noOp = new AssignOperator(unnest.getVariable(), varExp);
            noOp.getInputs().addAll(unnest.getInputs());
            opRef.setValue(noOp);
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
    private boolean updateDataSource(VXQueryCollectionDataSource ds, Mutable<ILogicalExpression> expression) {
        boolean added = false;
        List<Mutable<ILogicalExpression>> finds = new ArrayList<Mutable<ILogicalExpression>>();
        ExpressionToolbox.findAllFunctionExpressions(expression, BuiltinOperators.CHILD.getFunctionIdentifier(), finds);
        for (int i = finds.size(); i > 0; --i) {
            int typeId = ExpressionToolbox.getTypeExpressionTypeArgument(finds.get(i - 1));
            if (typeId > 0) {
                ds.addChildSeq(typeId);
                added = true;
            }
        }
        return added;
    }
}
