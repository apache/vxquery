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