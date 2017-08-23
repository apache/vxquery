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
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.metadata.AbstractVXQueryDataSource;
import org.apache.vxquery.metadata.IVXQueryDataSource;

/**
 * The rule searches for an unnest operator immediately following a data scan
 * operator.
 *
 * <pre>
 * Before
 *
 *   plan__parent
 *   UNNEST( $v2 : keys-or-members( $v1 ) )
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
public class PushKeysOrMembersIntoDatascanRule extends AbstractPushExpressionIntoDatascanRule {

    @Override
    boolean updateDataSource(IVXQueryDataSource datasource, Mutable<ILogicalExpression> expression) {
        AbstractVXQueryDataSource ds = (AbstractVXQueryDataSource) datasource;
        boolean added = false;
        BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        List<Mutable<ILogicalExpression>> findkeys = new ArrayList<>();
        ExpressionToolbox.findAllFunctionExpressions(expression,
                BuiltinOperators.KEYS_OR_MEMBERS.getFunctionIdentifier(), findkeys);
        for (int i = findkeys.size(); i > 0; --i) {
            XDMConstants.setTrue(bp);
            ds.appendValueSequence(bp);
            added = true;
        }
        return added;
    }

    @Override
    LogicalOperatorTag getOperator() {
        return LogicalOperatorTag.UNNEST;
    }
}
