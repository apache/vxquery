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
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;

/**
 * Find the collection functions and generate the data source objects.
 * Search pattern: unnest <- assign [function-call: collection] <- assign [constant: string]
 */
public class SetCollectionDataSourceRule extends AbstractCollectionRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        vxqueryContext.incrementTotalDataSources();
        context.addToDontApplySet(this, opRef.getValue());
        return false;
    }
}
