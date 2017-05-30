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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.metadata.IVXQueryDataSource;
import org.apache.vxquery.types.ElementType;

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
 *   ASSIGN( $v2 : $v1 ) 
 *   DATASCAN( $source : $v1 )
 *   plan__child
 *
 *   $source is encoded with the child parameters.
 * </pre>
 */
public class PushChildIntoDataScanRule extends AbstractPushExpressionIntoDatascanRule {

    @Override
    boolean updateDataSource(IVXQueryDataSource datasource, Mutable<ILogicalExpression> expression) {
        List<Mutable<ILogicalExpression>> finds = new ArrayList<Mutable<ILogicalExpression>>();
        boolean added = false;

        ExpressionToolbox.findAllFunctionExpressions(expression, BuiltinOperators.CHILD.getFunctionIdentifier(), finds);
        for (int i = finds.size(); i > 0; --i) {
            int typeId = ExpressionToolbox.getTypeExpressionTypeArgument(finds.get(i - 1));
            if (typeId > 0) {
                ElementType it = (ElementType) dCtx.lookupSequenceType(typeId).getItemType();
                ElementType et = ElementType.ANYELEMENT;

                if (it.getContentType().equals(et.getContentType())) {
                    datasource.addChildSeq(typeId);
                    added = true;
                }
            }
        }
        return added;
    }

    @Override
    LogicalOperatorTag getOperator() {
        return LogicalOperatorTag.UNNEST;
    }

}
