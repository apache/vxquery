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

import org.apache.vxquery.functions.BuiltinFunctions;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractVXQueryAggregateRule implements IAlgebraicRewriteRule {

    protected IFunctionInfo getAggregateFunction(AbstractFunctionCallExpression functionCall) {
        if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_COUNT_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_COUNT_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_AVG_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_AVG_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MIN_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_MIN_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_MAX_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_MAX_1;
        } else if (functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FN_SUM_1.getFunctionIdentifier())) {
            return BuiltinFunctions.FN_SUM_1;
        } else {
            return null;
        }
    }
}
