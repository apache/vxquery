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

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.cardinality.Cardinality;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;

public class VXQueryOptimizationContext extends AlgebricksOptimizationContext {

    private final Map<ILogicalOperator, HashMap<Integer, DocumentOrder>> documentOrderOperatorVariableMap = new HashMap<>();
    private final Map<ILogicalOperator, HashMap<Integer, UniqueNodes>> uniqueNodesOperatorVariableMap = new HashMap<>();
    private final Map<ILogicalOperator, Cardinality> cardinalityOperatorMap = new HashMap<>();

    private int totalDataSources = 0;
    private int collectionId = 0;

    public VXQueryOptimizationContext(int varCounter, IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer missableTypeComputer,
            IConflictingTypeResolver conflictingTypeResovler, PhysicalOptimizationConfig physicalOptimizationConfig,
            AlgebricksPartitionConstraint clusterLocations, LogicalOperatorPrettyPrintVisitor prettyPrintVisitor) {
        super(varCounter, expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                missableTypeComputer, conflictingTypeResovler, physicalOptimizationConfig, clusterLocations, prettyPrintVisitor);
    }

    public void incrementTotalDataSources() {
        totalDataSources++;
    }

    public int getTotalDataSources() {
        return totalDataSources;
    }

    public int newCollectionId() {
        return ++collectionId;
    }

    public Cardinality getCardinalityOperatorMap(ILogicalOperator op) {
        if (cardinalityOperatorMap.containsKey(op)) {
            return cardinalityOperatorMap.get(op);
        } else {
            return null;
        }
    }

    public void putCardinalityOperatorMap(ILogicalOperator op, Cardinality cardinality) {
        this.cardinalityOperatorMap.put(op, cardinality);
    }

    public HashMap<Integer, DocumentOrder> getDocumentOrderOperatorVariableMap(ILogicalOperator op) {
        if (documentOrderOperatorVariableMap.containsKey(op)) {
            return documentOrderOperatorVariableMap.get(op);
        } else {
            return null;
        }
    }

    public void putDocumentOrderOperatorVariableMap(ILogicalOperator op, HashMap<Integer, DocumentOrder> variableMap) {
        this.documentOrderOperatorVariableMap.put(op, variableMap);
    }

    public HashMap<Integer, UniqueNodes> getUniqueNodesOperatorVariableMap(ILogicalOperator op) {
        if (uniqueNodesOperatorVariableMap.containsKey(op)) {
            return uniqueNodesOperatorVariableMap.get(op);
        } else {
            return null;
        }
    }

    public void putUniqueNodesOperatorVariableMap(ILogicalOperator op, HashMap<Integer, UniqueNodes> variableMap) {
        this.uniqueNodesOperatorVariableMap.put(op, variableMap);
    }

}
