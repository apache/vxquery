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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.metadata.IVXQueryDataSource;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;

/**
 * The rule searches for an assign operator immediately following a data scan
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

public class PushValueIntoDatascanRule extends AbstractPushExpressionIntoDatascanRule {
    TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();

    @Override
    boolean updateDataSource(IVXQueryDataSource datasource, Mutable<ILogicalExpression> expression) {
        if (datasource.usingIndex()) {
            return false;
        }
        VXQueryCollectionDataSource ds = (VXQueryCollectionDataSource) datasource;
        boolean added = false;
        List<Mutable<ILogicalExpression>> finds = new ArrayList<>();
        ILogicalExpression le = expression.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            if (afce.getFunctionIdentifier().equals(BuiltinFunctions.FN_ZERO_OR_ONE_1.getFunctionIdentifier())) {
                return false;
            }
        }
        ExpressionToolbox.findAllFunctionExpressions(expression, BuiltinOperators.VALUE.getFunctionIdentifier(), finds);

        for (int i = finds.size(); i > 0; --i) {
            List<ILogicalExpression> values = ExpressionToolbox.getFullArguments(finds.get(i - 1));
            if (values.size() > 1) {
                ExpressionToolbox.getConstantArgument(finds.get(i - 1), 1, tvp);
                ds.appendValueSequence(tvp);
                added = true;
            }
        }

        return added;
    }

    @Override
    LogicalOperatorTag getOperator() {
        return LogicalOperatorTag.ASSIGN;
    }

}
