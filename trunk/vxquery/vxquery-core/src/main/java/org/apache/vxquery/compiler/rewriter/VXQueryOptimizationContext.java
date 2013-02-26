/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.rewriter;

import java.util.HashMap;
import java.util.Map;

import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class VXQueryOptimizationContext extends AlgebricksOptimizationContext {

    private final Map<ILogicalOperator, HashMap<Integer, DocumentOrder>> documentOrderOperatorVariableMap = new HashMap<ILogicalOperator, HashMap<Integer, DocumentOrder>>();
    private final Map<ILogicalOperator, HashMap<Integer, UniqueNodes>> uniqueNodesOperatorVariableMap = new HashMap<ILogicalOperator, HashMap<Integer, UniqueNodes>>();

    public VXQueryOptimizationContext(int varCounter, int frameSize,
            IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
            PhysicalOptimizationConfig physicalOptimizationConfig) {
        super(varCounter, frameSize, expressionEvalSizeComputer, mergeAggregationExpressionFactory,
                expressionTypeComputer, nullableTypeComputer, physicalOptimizationConfig);
    }

    public HashMap<Integer, DocumentOrder> getDocumentOrderOperatorVariableMap(ILogicalOperator op) {
        return documentOrderOperatorVariableMap.get(op);
    }

    public void putDocumentOrderOperatorVariableMap(ILogicalOperator op, HashMap<Integer, DocumentOrder> variableMap) {
        this.documentOrderOperatorVariableMap.put(op, variableMap);
    }

    public HashMap<Integer, UniqueNodes> getUniqueNodesOperatorVariableMap(ILogicalOperator op) {
        return uniqueNodesOperatorVariableMap.get(op);
    }

    public void putUniqueNodesOperatorVariableMap(ILogicalOperator op, HashMap<Integer, UniqueNodes> variableMap) {
        this.uniqueNodesOperatorVariableMap.put(op, variableMap);
    }

}