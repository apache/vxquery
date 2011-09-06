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
import java.util.List;

import org.apache.vxquery.compiler.rewriter.framework.RewriteRule;
import org.apache.vxquery.compiler.rewriter.framework.RulesetProvider;
import org.apache.vxquery.compiler.rewriter.rules.LoggingRewriteRule;

public class LoggingRulesetProviderImpl implements RulesetProvider {
    private final RulesetProvider delegate;

    public LoggingRulesetProviderImpl(RulesetProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<RewriteRule> createRuleset() {
        List<RewriteRule> loggingRules = new ArrayList<RewriteRule>();
        for (RewriteRule rule : delegate.createRuleset()) {
            loggingRules.add(new LoggingRewriteRule(rule));
        }
        return loggingRules;
    }
}