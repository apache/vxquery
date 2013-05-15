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
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.metadata.VXQueryCollectionDataSource;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;

public class SetCollectionDataSourceRule extends AbstractCollectionRule {
    /**
     * Find the collection functions and generate the data source objects.
     * Search pattern: unnest <- assign [function-call: collection] <- assign [constant: string]
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        String collectionName = getCollectionName(opRef);

        // Build the new collection.
        if (collectionName != null && vxqueryContext.getCollectionDataSourceMap(collectionName) == null) {
            int collectionId = vxqueryContext.getCollectionDataSourceMapSize() + 1;
            List<Object> types = new ArrayList<Object>();
            types.add(SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR));

            VXQueryCollectionDataSource ds = new VXQueryCollectionDataSource(collectionId, collectionName,
                    types.toArray());
            vxqueryContext.putCollectionDataSourceMap(collectionName, ds);
        }
        return false;
    }
}
