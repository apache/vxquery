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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.metadata.VXQueryIndexingDataSource;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class IntroduceIndexingRule extends AbstractCollectionRule {
    private static final Set<FunctionIdentifier> IDENTIFIERS = new HashSet<>();
    static {
        IDENTIFIERS.add(BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_2.getFunctionIdentifier());
        IDENTIFIERS.add(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier());
        IDENTIFIERS.add(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier());
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        String args[] = getFunctionalArguments(opRef, IDENTIFIERS);


        if (args != null) {

            // Check if the function call is for build-collection-on-index.
            // In build-collection-on-index, args[0] contains collection and args[1] contains index.
            // In all other queries, args[0] contains index.
            String index;
            String collection;
            if (args.length == 2) {
                collection = args[0];
                index = args[1];
            } else {
                collection = null;
                index = args[0];
            }

            // Build the new operator and update the query plan.
            int collectionId = vxqueryContext.newCollectionId();
            VXQueryIndexingDataSource ids = VXQueryIndexingDataSource.create(collectionId, collection, index,
                    SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR),
                    functionCall.getFunctionIdentifier().getName());
            if (ids != null) {
                ids.setTotalDataSources(vxqueryContext.getTotalDataSources());

//                // Check if the call is for build-index-on-collection
//                if (args.length == 2) {
//                    ids.setTotalDataSources(vxqueryContext.getTotalDataSources());
//                    ids.setTag(args[1]);
//                }

                // Known to be true because of collection name.
                AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
                UnnestOperator unnest = (UnnestOperator) op;
                Mutable<ILogicalOperator> opRef2 = unnest.getInputs().get(0);
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
                AssignOperator assign = (AssignOperator) op2;

                DataSourceScanOperator opNew = new DataSourceScanOperator(assign.getVariables(), ids);
                opNew.getInputs().addAll(assign.getInputs());
                opRef2.setValue(opNew);
                return true;
            }
        }
        return false;
    }
}
