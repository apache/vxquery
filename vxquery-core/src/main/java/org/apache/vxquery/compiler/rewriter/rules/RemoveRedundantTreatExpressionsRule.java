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

import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.runtime.functions.type.SequenceTypeMatcher;
import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * The rule searches for where the xquery treat function is used. When the
 * expression's return type matches the treat expression type, the treat is
 * removed.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : treat( \@input_expression, \@type_expression ) )
 *   plan__child
 *   
 *   Where treat \@type_expression is the same as the return type of \@input_expression.
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

public class RemoveRedundantTreatExpressionsRule extends AbstractRemoveRedundantTypeExpressionsRule {
    final SequenceTypeMatcher stm = new SequenceTypeMatcher();

    protected FunctionIdentifier getSearchFunction() {
        return BuiltinOperators.TREAT.getFunctionIdentifier();
    }
    
    protected boolean getTreatFunction() {
        return true;
    }

    @Override
    public boolean matchesAllInstancesOf(SequenceType sTypeArg, SequenceType sTypeOutput) {
        if (sTypeArg != null) {
            stm.setSequenceType(sTypeArg);
            if (sTypeOutput != null && stm.matchesAllInstances(sTypeOutput)) {
                return true;
            }
        }
        return false;
    }
}
