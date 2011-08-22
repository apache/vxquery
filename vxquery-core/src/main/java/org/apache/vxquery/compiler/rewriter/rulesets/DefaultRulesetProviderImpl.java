/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.rewriter.rulesets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.vxquery.compiler.rewriter.framework.RewriteRule;
import org.apache.vxquery.compiler.rewriter.framework.RulesetProvider;
import org.apache.vxquery.compiler.rewriter.rules.EliminateIfThenElseDeadCode;
import org.apache.vxquery.compiler.rewriter.rules.EliminateUnusedLetVariables;
import org.apache.vxquery.compiler.rewriter.rules.EvaluateInvariantInstanceofExpressions;

public class DefaultRulesetProviderImpl implements RulesetProvider {
    public static final RulesetProvider INSTANCE = new DefaultRulesetProviderImpl();

    List<RewriteRule> rules;

    private DefaultRulesetProviderImpl() {
        List<RewriteRule> temp = new ArrayList<RewriteRule>();
        temp.add(new EliminateUnusedLetVariables(1));
        temp.add(new EvaluateInvariantInstanceofExpressions(1));
        temp.add(new EliminateIfThenElseDeadCode(1));

        rules = Collections.unmodifiableList(temp);
    }

    @Override
    public List<RewriteRule> createRuleset() {
        return rules;
    }
}