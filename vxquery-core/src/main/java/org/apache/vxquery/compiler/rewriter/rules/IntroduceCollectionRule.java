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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.vxquery.common.VXQueryCommons;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
import org.apache.vxquery.metadata.VXQueryIndexingDataSource;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

/**
 * Find the default query plan created for collection and updated it to use
 * parallelization. The rule searches for unnest followed by an assign for the
 * collection function expression. When this plan block exists the data source
 * scan operator added in the blocks place.
 *
 * <pre>
 * Before
 *
 *   plan__parent
 *   UNNEST( $v2 : exp($v1) )
 *   ASSIGN( $v1 : collection( $v0 ) )
 *   ASSIGN( $v0 : constant )
 *   plan__child
 *
 * After
 *
 *   plan__parent
 *   UNNEST( $v2 : exp($v1) )
 *   DATASCAN( collection( $v0 ) , $v1 )
 *   plan__child
 *
 *   Where DATASCAN operator is configured to use the collection( $v0) for
 *   data represented by the "constant" and $v1 represents the xml document
 *   nodes from the collection.
 * </pre>
 *
 * @author prestonc
 */
public class IntroduceCollectionRule extends AbstractCollectionRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        String[] args = getFunctionalArguments(opRef, VXQueryCommons.collectionFunctions);
        VXQueryMetadataProvider metadata = (VXQueryMetadataProvider) context.getMetadataProvider();
        if (args != null) {
            String collectionName = args[0];
            // Build the new operator and update the query plan.
            int collectionId = vxqueryContext.newCollectionId();
            ArrayList<String> collectionTempName = new ArrayList<String>();
            collectionTempName.add(collectionName);
            if (collectionName.contains("|")) {
                collectionTempName.remove(0);
                int index = collectionName.indexOf("|");
                int start = 0;
                while (index >= 0) {
                    collectionTempName.add(collectionName.substring(start, index));
                    start = index + 1;
                    index = collectionName.indexOf("|", index + 1);
                    if (index == -1) {
                        collectionTempName.add(collectionName.substring(start));
                    }
                }
            }
            if (metadata.hasIndex(collectionTempName)) {
                VXQueryIndexingDataSource ids = VXQueryIndexingDataSource.create(collectionId, collectionName,
                        SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR),
                        functionCall.getFunctionIdentifier().getName());
                if (ids != null) {
                    ids.setTotalDataSources(vxqueryContext.getTotalDataSources());
                    return setDataSourceScan(ids, opRef);
                }
            }
            VXQueryCollectionDataSource ds = VXQueryCollectionDataSource.create(collectionId, collectionName,
                    SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR));
            if (ds != null) {
                ds.setTotalDataSources(vxqueryContext.getTotalDataSources());

                // Check if the call is for collection-with-tag
                if (args.length == 2) {
                    ds.setTotalDataSources(vxqueryContext.getTotalDataSources());
                    ds.setTag(args[1]);
                }

                // Known to be true because of collection name.
                return setDataSourceScan(ds, opRef);
            }
        }
        return false;
    }

}
