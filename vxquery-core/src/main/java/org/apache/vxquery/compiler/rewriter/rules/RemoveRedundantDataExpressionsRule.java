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
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * The rule searches for where the xquery data function is used. When the
 * expression's return type is atomic, the data function is removed.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : data( \@input_expression ) )
 *   plan__child
 *   
 *   Where \@input_expression creates an atomic value.
 *   
 * After 
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : \@input_expression )
 *   plan__child
 * </pre>
 * 
 * @author prestonc
 */
public class RemoveRedundantDataExpressionsRule extends AbstractRemoveRedundantTypeExpressionsRule {
    protected FunctionIdentifier getSearchFunction() {
        return BuiltinFunctions.FN_DATA_1.getFunctionIdentifier();
    }

    @Override
    public boolean hasTypeArgument() {
        return false;
    }

    public boolean safeToReplace(SequenceType sTypeArg, SequenceType sTypeOutput) {
        if (sTypeOutput != null && sTypeOutput.getItemType().isAtomicType()) {
            return true;
        }
        return false;
    }
}
