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
package org.apache.vxquery.compiler.rewriter.rules.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;

public class OperatorToolbox {

    public static Mutable<ILogicalOperator> findLastSubplanOperator(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef;
        AbstractLogicalOperator next;
        while (op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef;
            next = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            if (next.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                break;
            }
        }
        return opRef;
    }

    public static AbstractLogicalOperator findLastSubplanOperator(AbstractLogicalOperator op) {
        AbstractLogicalOperator next;
        while (op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            next = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            if (next.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                break;
            }
        }
        return op;
    }

    public static List<Mutable<ILogicalExpression>> getExpressions(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        List<Mutable<ILogicalExpression>> result = new ArrayList<Mutable<ILogicalExpression>>();
        switch (op.getOperatorTag()) {
            case AGGREGATE:
            case ASSIGN:
            case RUNNINGAGGREGATE:
                AbstractAssignOperator aao = (AbstractAssignOperator) op;
                result.addAll(aao.getExpressions());
                break;
            case INNERJOIN:
            case LEFTOUTERJOIN:
                AbstractBinaryJoinOperator abjo = (AbstractBinaryJoinOperator) op;
                result.add(abjo.getCondition());
                break;
            case SELECT:
                SelectOperator so = (SelectOperator) op;
                result.add(so.getCondition());
                break;
            case UNNEST:
            case UNNEST_MAP:
                AbstractUnnestOperator auo = (AbstractUnnestOperator) op;
                result.add(auo.getExpressionRef());
                break;
            default:
                // TODO Not yet implemented.
                break;
        }
        return result;
    }

    public static Mutable<ILogicalExpression> getExpressionOf(Mutable<ILogicalOperator> opRef, LogicalVariable lv) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case AGGREGATE:
            case ASSIGN:
            case RUNNINGAGGREGATE:
                AbstractAssignOperator aao = (AbstractAssignOperator) op;
                if (!aao.getVariables().contains(lv)) {
                    return null;
                }
                return aao.getExpressions().get(aao.getVariables().indexOf(lv));
            case UNNEST:
            case UNNEST_MAP:
                AbstractUnnestOperator ano = (AbstractUnnestOperator) op;
                return ano.getExpressionRef();
            default:
                // TODO Not yet implemented.
                break;
        }
        return null;
    }

    public static Mutable<ILogicalOperator> findProducerOf(Mutable<ILogicalOperator> opRef, LogicalVariable lv) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case AGGREGATE:
            case ASSIGN:
            case RUNNINGAGGREGATE:
                AbstractAssignOperator aao = (AbstractAssignOperator) op;
                if (aao.getVariables().contains(lv)) {
                    return opRef;
                }
                for (Mutable<ILogicalOperator> input : op.getInputs()) {
                    Mutable<ILogicalOperator> opInput = findProducerOf(input, lv);
                    if (opInput != null) {
                        return opInput;
                    }
                }
                break;
            case DATASOURCESCAN:
            case UNNEST:
            case UNNEST_MAP:
                AbstractScanOperator aso = (AbstractScanOperator) op;
                if (aso.getVariables().contains(lv)) {
                    return opRef;
                }
                for (Mutable<ILogicalOperator> input : op.getInputs()) {
                    Mutable<ILogicalOperator> opInput = findProducerOf(input, lv);
                    if (opInput != null) {
                        return opInput;
                    }
                }
                break;
            case NESTEDTUPLESOURCE:
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
                return findProducerOf(nts.getDataSourceReference(), lv);
            case EMPTYTUPLESOURCE:
                return null;
            default:
                // Skip operators and go look at input.
                for (Mutable<ILogicalOperator> input : op.getInputs()) {
                    Mutable<ILogicalOperator> opInput = findProducerOf(input, lv);
                    if (opInput != null) {
                        return opInput;
                    }
                }
                break;
        }
        return null;
    }
}
