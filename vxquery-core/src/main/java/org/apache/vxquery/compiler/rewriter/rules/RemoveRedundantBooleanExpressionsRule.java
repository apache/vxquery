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
import org.apache.vxquery.runtime.functions.type.SequenceTypeMatcher;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * The rule searches for where the xquery boolean function is used. When the
 * expressions input is already a boolean value, remove the boolean function.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : boolean( \@input_expression ) )
 *   plan__child
 *   
 *   Where treat \@input_expression is known to be a boolean value.
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

public class RemoveRedundantBooleanExpressionsRule extends AbstractRemoveRedundantTypeExpressionsRule {
    final SequenceTypeMatcher stm = new SequenceTypeMatcher();

    protected FunctionIdentifier getSearchFunction() {
        return BuiltinFunctions.FN_BOOLEAN_1.getFunctionIdentifier();
    }

    @Override
    public boolean hasTypeArgument() {
        return false;
    }

    public boolean matchesAllInstancesOf(SequenceType sTypeArg, SequenceType sTypeOutput) {
        stm.setSequenceType(SequenceType.create(BuiltinTypeRegistry.XS_BOOLEAN, Quantifier.QUANT_ONE));
        if (sTypeOutput != null && stm.matchesAllInstances(sTypeOutput)) {
            return true;
        }
        return false;
    }
}
