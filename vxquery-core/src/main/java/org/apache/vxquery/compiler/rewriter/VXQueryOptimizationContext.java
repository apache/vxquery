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

import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.cardinality.Cardinality;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class VXQueryOptimizationContext extends AlgebricksOptimizationContext {

    private final Map<ILogicalOperator, HashMap<Integer, DocumentOrder>> documentOrderOperatorVariableMap = new HashMap<ILogicalOperator, HashMap<Integer, DocumentOrder>>();
    private final Map<ILogicalOperator, HashMap<Integer, UniqueNodes>> uniqueNodesOperatorVariableMap = new HashMap<ILogicalOperator, HashMap<Integer, UniqueNodes>>();
    private final Map<ILogicalOperator, Cardinality> cardinalityOperatorMap = new HashMap<ILogicalOperator, Cardinality>();

    private final Map<String, VXQueryCollectionDataSource> dataSourceScanMap = new HashMap<String, VXQueryCollectionDataSource>();

    public VXQueryOptimizationContext(int varCounter, IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
            PhysicalOptimizationConfig physicalOptimizationConfig, LogicalOperatorPrettyPrintVisitor prettyPrintVisitor) {
        super(varCounter, expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                nullableTypeComputer, physicalOptimizationConfig, prettyPrintVisitor);
    }

    public VXQueryCollectionDataSource getCollectionDataSourceMap(String collectionName) {
        if (dataSourceScanMap.containsKey(collectionName)) {
            return dataSourceScanMap.get(collectionName);
        } else {
            return null;
        }
    }

    public int getCollectionDataSourceMapSize() {
        return dataSourceScanMap.size();
    }

    public void putCollectionDataSourceMap(String collectionName, VXQueryCollectionDataSource ds) {
        this.dataSourceScanMap.put(collectionName, ds);
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