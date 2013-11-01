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
package org.apache.vxquery.compiler.rewriter.rules;

import org.apache.vxquery.compiler.rewriter.rules.InlineReferenceVariablesRule.IInlineReferenceVariablePolicy;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

/**
 * The inline reference variable rule searches for assignments that only create
 * a new variable ID and then update all reference for that variable to be the
 * original variable. This rule is immediately followed by a rule to remove
 * unused assign variables.
 * 
 * <pre>
 * Before
 * 
 *  plan__parent
 *  ASSIGN( $v : reference_variable($vr) )
 *  plan__child
 *  
 *  Where $vr is a reference variable defined in plan__child.
 *  
 * After
 * 
 *  plan__parent_new
 *  ASSIGN( $v : reference_variable($vr) )
 *  plan__child
 *  
 *  such that plan__parent_new = {for each $v in plan__parent is replaced with $vr}
 * </pre>
 */
public class InlineReferenceVariablePolicy implements IInlineReferenceVariablePolicy {

    @Override
    public boolean isCandidateForInlining(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isCanidateInlineTarget(AbstractLogicalOperator op) {
        return true;
    }

}
