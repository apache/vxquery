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

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;

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
    
    public static List<Mutable<ILogicalExpression>> getExpression(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        List<Mutable<ILogicalExpression>> result = new ArrayList<Mutable<ILogicalExpression>>();
        switch (op.getOperatorTag()) {
            case AGGREGATE:
            case ASSIGN:
            case RUNNINGAGGREGATE:
                AbstractAssignOperator aao = (AbstractAssignOperator) op;
                result.addAll(aao.getExpressions());
                break;
            case UNNEST:
            case UNNEST_MAP:
                AbstractUnnestOperator auo = (AbstractUnnestOperator) op;
                result.add(auo.getExpressionRef());
                break;
            case CLUSTER:
            case DATASOURCESCAN:
            case DISTINCT:
            case DISTRIBUTE_RESULT:
            case EMPTYTUPLESOURCE:
            case EXCHANGE:
            case EXTENSION_OPERATOR:
            case GROUP:
            case INDEX_INSERT_DELETE:
            case INNERJOIN:
            case INSERT_DELETE:
            case LEFTOUTERJOIN:
            case LIMIT:
            case NESTEDTUPLESOURCE:
            case ORDER:
            case PARTITIONINGSPLIT:
            case PROJECT:
            case REPLICATE:
            case SCRIPT:
            case SELECT:
            case SINK:
            case SUBPLAN:
            case UNIONALL:
            case UPDATE:
            case WRITE:
            case WRITE_RESULT:
            default:
                break;
        }
        return result;
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
            case UNNEST:
            case UNNEST_MAP:
                AbstractUnnestOperator auo = (AbstractUnnestOperator) op;
                if (auo.getVariables().contains(lv)) {
                    return opRef;
                }
                for (Mutable<ILogicalOperator> input : op.getInputs()) {
                    Mutable<ILogicalOperator> opInput = findProducerOf(input, lv);
                     if (opInput != null) {
                        return opInput;
                    }
                }
                break;
            case EMPTYTUPLESOURCE:
            case NESTEDTUPLESOURCE:
                return null;
            case CLUSTER:
            case DATASOURCESCAN:
            case DISTINCT:
            case DISTRIBUTE_RESULT:
            case EXCHANGE:
            case EXTENSION_OPERATOR:
            case GROUP:
            case INDEX_INSERT_DELETE:
            case INNERJOIN:
            case INSERT_DELETE:
            case LEFTOUTERJOIN:
            case LIMIT:
            case ORDER:
            case PARTITIONINGSPLIT:
            case PROJECT:
            case REPLICATE:
            case SCRIPT:
            case SELECT:
            case SINK:
            case SUBPLAN:
            case UNIONALL:
            case UPDATE:
            case WRITE:
            case WRITE_RESULT:
            default:
                // TODO Not yet implemented.
                break;
        }
        return null;
    }
}
