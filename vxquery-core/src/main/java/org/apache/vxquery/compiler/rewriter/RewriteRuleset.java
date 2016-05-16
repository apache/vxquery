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
package org.apache.vxquery.compiler.rewriter;

import java.util.LinkedList;
import java.util.List;

import org.apache.vxquery.compiler.rewriter.rules.CollectionWithTagRule;
import org.apache.vxquery.compiler.rewriter.rules.ConsolidateAssignAggregateRule;
import org.apache.vxquery.compiler.rewriter.rules.ConsolidateDescandantChild;
import org.apache.vxquery.compiler.rewriter.rules.ConvertAssignToUnnestRule;
import org.apache.vxquery.compiler.rewriter.rules.ConvertFromAlgebricksExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.ConvertToAlgebricksExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateSubplanForSingleItemsRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateUnnestAggregateSequencesRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateUnnestAggregateSubplanRule;
import org.apache.vxquery.compiler.rewriter.rules.IntroduceCollectionRule;
import org.apache.vxquery.compiler.rewriter.rules.IntroduceTwoStepAggregateRule;
import org.apache.vxquery.compiler.rewriter.rules.PushChildIntoDataScanRule;
import org.apache.vxquery.compiler.rewriter.rules.PushFunctionsOntoEqJoinBranches;
import org.apache.vxquery.compiler.rewriter.rules.RemoveRedundantBooleanExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveRedundantCastExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveRedundantDataExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveRedundantPromoteExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveRedundantTreatExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveUnusedSortDistinctNodesRule;
import org.apache.vxquery.compiler.rewriter.rules.RemoveUnusedUnnestIterateRule;
import org.apache.vxquery.compiler.rewriter.rules.ReplaceSourceMapInDocExpression;
import org.apache.vxquery.compiler.rewriter.rules.SetCollectionDataSourceRule;
import org.apache.vxquery.compiler.rewriter.rules.SetVariableIdContextRule;
import org.apache.vxquery.compiler.rewriter.rules.algebricksalternatives.ExtractFunctionsFromJoinConditionRule;
import org.apache.vxquery.compiler.rewriter.rules.algebricksalternatives.InlineNestedVariablesRule;
import org.apache.vxquery.compiler.rewriter.rules.algebricksalternatives.MoveFreeVariableOperatorOutOfSubplanRule;

import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexJoinInferenceRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.CopyLimitDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateGroupByEmptyKeyRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateSubplanWithInputCardinalityOneRule;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import org.apache.hyracks.algebricks.rewriter.rules.InferTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineAssignIntoAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InsertOuterJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroJoinInsideSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceGroupByCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceGroupByForSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.NestedSubplanToJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import org.apache.hyracks.algebricks.rewriter.rules.PushMapOperatorDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectIntoDataSourceScanRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanIntoGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanWithAggregateDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;
import org.apache.hyracks.algebricks.rewriter.rules.SimpleUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.SubplanOutOfGroupRule;

public class RewriteRuleset {
    /**
     * Optimizations specific to XQuery.
     *
     * @return List of algebraic rewrite rules.
     */
    public final static List<IAlgebraicRewriteRule> buildPathStepNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();
        normalization.add(new SetVariableIdContextRule());

        // Remove unused functions.
        normalization.add(new RemoveUnusedSortDistinctNodesRule());
        normalization.add(new RemoveRedundantTreatExpressionsRule());
        normalization.add(new RemoveRedundantDataExpressionsRule());
        normalization.add(new RemoveRedundantPromoteExpressionsRule());
        normalization.add(new RemoveRedundantCastExpressionsRule());
        normalization.add(new RemoveRedundantBooleanExpressionsRule());
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        // TODO Fix the group by operator before putting back in the rule set.
        //        normalization.add(new ConvertAssignSortDistinctNodesToOperatorsRule());

        // Find unnest followed by aggregate in a subplan.
        normalization.add(new EliminateUnnestAggregateSubplanRule());
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        // Remove single tuple input subplans and merge unnest aggregate operators.
        // TODO Fix EliminateSubplanForSinglePathsRule to check for variables used after the subplan.
        //        normalization.add(new EliminateSubplanForSinglePathsRule());
        normalization.add(new EliminateUnnestAggregateSequencesRule());

        normalization.add(new ConvertAssignToUnnestRule());

        // Used to clean up any missing noops after all the subplans have been altered.
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        // Convert to a data source scan operator.
        normalization.add(new SetCollectionDataSourceRule());
        normalization.add(new IntroduceCollectionRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());
        normalization.add(new CollectionWithTagRule());

        normalization.add(new ConsolidateDescandantChild());

        normalization.add(new ReplaceSourceMapInDocExpression());
        // Adds child steps to the data source scan.
        // TODO Replace consolidate with a new child function that takes multiple paths.
        //        normalization.add(new ConsolidateUnnestsRule());
        normalization.add(new RemoveUnusedUnnestIterateRule());
        normalization.add(new PushChildIntoDataScanRule());

        // Improvement for scalar child expressions
        normalization.add(new EliminateSubplanForSingleItemsRule());
        normalization.add(new MoveFreeVariableOperatorOutOfSubplanRule());
        return normalization;
    }

    /**
     * Optimizations specific to XQuery.
     *
     * @return List of algebraic rewrite rules.
     */
    public final static List<IAlgebraicRewriteRule> buildXQueryNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();

        // Find assign for scalar aggregate function followed by an aggregate operator.
        normalization.add(new ConsolidateAssignAggregateRule());
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        // Find assign for scalar aggregate function.
        // normalization.add(new ConvertAssignToAggregateRule());

        // Use two step aggregate operators if possible.
        normalization.add(new IntroduceTwoStepAggregateRule());

        // Used to clean up any missing noops after all the subplans have been altered.
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        return normalization;
    }

    /**
     * Remove expressions known to be redundant.
     *
     * @return List of algebraic rewrite rules.
     */
    public final static List<IAlgebraicRewriteRule> buildInlineRedundantExpressionNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();
        normalization.add(new InlineNestedVariablesRule());
        normalization.add(new RemoveRedundantTreatExpressionsRule());
        normalization.add(new RemoveRedundantDataExpressionsRule());
        normalization.add(new RemoveRedundantPromoteExpressionsRule());
        normalization.add(new RemoveRedundantCastExpressionsRule());
        normalization.add(new ConvertToAlgebricksExpressionsRule());
        normalization.add(new RemoveRedundantBooleanExpressionsRule());
        // Clean up
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());
        return normalization;
    }

    /**
     * When a nested data sources exist, convert the plan to use the join operator.
     *
     * @return List of algebraic rewrite rules.
     */
    public final static List<IAlgebraicRewriteRule> buildNestedDataSourceRuleCollection() {
        List<IAlgebraicRewriteRule> xquery = new LinkedList<IAlgebraicRewriteRule>();
        xquery.add(new BreakSelectIntoConjunctsRule());
        xquery.add(new SimpleUnnestToProductRule());
        xquery.add(new PushMapOperatorDownThroughProductRule());
        xquery.add(new PushSubplanWithAggregateDownThroughProductRule());
        xquery.add(new PushSelectDownRule());
        xquery.add(new PushSelectIntoJoinRule());
        // Clean up
        xquery.add(new RemoveRedundantVariablesRule());
        xquery.add(new RemoveUnusedAssignAndAggregateRule());
        return xquery;
    }

    public final static List<IAlgebraicRewriteRule> buildTypeInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<IAlgebraicRewriteRule>();
        typeInfer.add(new InferTypesRule());
        return typeInfer;
    }

    /**
     * Unnest more complex structures.
     *
     * @return List of algebraic rewrite rules.
     */
    public final static List<IAlgebraicRewriteRule> buildUnnestingRuleCollection() {
        List<IAlgebraicRewriteRule> xquery = new LinkedList<IAlgebraicRewriteRule>();

        xquery.add(new PushSelectDownRule());
        xquery.add(new SimpleUnnestToProductRule());
        xquery.add(new ComplexUnnestToProductRule());
        xquery.add(new ComplexJoinInferenceRule());
        xquery.add(new PushSelectIntoJoinRule());
        xquery.add(new IntroJoinInsideSubplanRule());
        xquery.add(new PushMapOperatorDownThroughProductRule());
        xquery.add(new PushSubplanWithAggregateDownThroughProductRule());
        xquery.add(new IntroduceGroupByForSubplanRule());
        xquery.add(new SubplanOutOfGroupRule());
        xquery.add(new InsertOuterJoinRule());
        xquery.add(new ExtractFunctionsFromJoinConditionRule());

        xquery.add(new RemoveRedundantVariablesRule());
        xquery.add(new RemoveUnusedAssignAndAggregateRule());

        xquery.add(new FactorRedundantGroupAndDecorVarsRule());
        xquery.add(new EliminateSubplanRule());
        xquery.add(new EliminateGroupByEmptyKeyRule());
        xquery.add(new PushSubplanIntoGroupByRule());
        xquery.add(new NestedSubplanToJoinRule());
        xquery.add(new EliminateSubplanWithInputCardinalityOneRule());

        return xquery;
    }

    public final static List<IAlgebraicRewriteRule> buildNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<IAlgebraicRewriteRule>();
        normalization.add(new EliminateSubplanRule());
        normalization.add(new BreakSelectIntoConjunctsRule());
        normalization.add(new PushSelectIntoJoinRule());
        normalization.add(new ExtractGbyExpressionsRule());
        return normalization;
    }

    public final static List<IAlgebraicRewriteRule> buildCondPushDownRuleCollection() {
        List<IAlgebraicRewriteRule> condPushDown = new LinkedList<IAlgebraicRewriteRule>();
        condPushDown.add(new PushSelectDownRule());
        condPushDown.add(new InlineVariablesRule());
        condPushDown.add(new FactorRedundantGroupAndDecorVarsRule());
        condPushDown.add(new EliminateSubplanRule());
        return condPushDown;
    }

    public final static List<IAlgebraicRewriteRule> buildJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> joinInference = new LinkedList<IAlgebraicRewriteRule>();
        joinInference.add(new InlineVariablesRule());
        joinInference.add(new ComplexJoinInferenceRule());
        return joinInference;
    }

    public final static List<IAlgebraicRewriteRule> buildOpPushDownRuleCollection() {
        List<IAlgebraicRewriteRule> opPushDown = new LinkedList<IAlgebraicRewriteRule>();
        opPushDown.add(new PushProjectDownRule());
        opPushDown.add(new PushSelectDownRule());
        return opPushDown;
    }

    public final static List<IAlgebraicRewriteRule> buildIntroduceProjectRuleCollection() {
        List<IAlgebraicRewriteRule> project = new LinkedList<IAlgebraicRewriteRule>();
        project.add(new IntroduceProjectsRule());
        return project;
    }

    public final static List<IAlgebraicRewriteRule> buildDataExchangeRuleCollection() {
        List<IAlgebraicRewriteRule> dataExchange = new LinkedList<IAlgebraicRewriteRule>();
        dataExchange.add(new SetExecutionModeRule());
        return dataExchange;
    }

    public final static List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> consolidation = new LinkedList<IAlgebraicRewriteRule>();
        consolidation.add(new ConsolidateSelectsRule());
        consolidation.add(new ConsolidateAssignsRule());
        consolidation.add(new InlineAssignIntoAggregateRule());
        consolidation.add(new IntroduceGroupByCombinerRule());
        consolidation.add(new IntroduceAggregateCombinerRule());
        consolidation.add(new RemoveUnusedAssignAndAggregateRule());
        return consolidation;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesAllLevelsRuleCollection() {
        List<IAlgebraicRewriteRule> physicalPlanRewrites = new LinkedList<IAlgebraicRewriteRule>();
        physicalPlanRewrites.add(new PullSelectOutOfEqJoin());
        physicalPlanRewrites.add(new PushFunctionsOntoEqJoinBranches());
        physicalPlanRewrites.add(new SetAlgebricksPhysicalOperatorsRule());
        physicalPlanRewrites.add(new SetExecutionModeRule());
        physicalPlanRewrites.add(new EnforceStructuralPropertiesRule());
        physicalPlanRewrites.add(new PushProjectDownRule());
        physicalPlanRewrites.add(new CopyLimitDownRule());
        return physicalPlanRewrites;
    }

    public final static List<IAlgebraicRewriteRule> buildPhysicalRewritesTopLevelRuleCollection() {
        List<IAlgebraicRewriteRule> physicalPlanRewrites = new LinkedList<IAlgebraicRewriteRule>();
        physicalPlanRewrites.add(new CopyLimitDownRule());
        physicalPlanRewrites.add(new SetExecutionModeRule());
        return physicalPlanRewrites;
    }

    public final static List<IAlgebraicRewriteRule> prepareForJobGenRuleCollection() {
        List<IAlgebraicRewriteRule> prepareForJobGenRewrites = new LinkedList<IAlgebraicRewriteRule>();
        prepareForJobGenRewrites.add(new ConvertFromAlgebricksExpressionsRule());
        prepareForJobGenRewrites
                .add(new IsolateHyracksOperatorsRule(HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled));
        prepareForJobGenRewrites.add(new ExtractCommonOperatorsRule());
        // Re-infer all types, so that, e.g., the effect of not-is-null is
        // propagated.
        prepareForJobGenRewrites.add(new PushProjectIntoDataSourceScanRule());
        prepareForJobGenRewrites.add(new ReinferAllTypesRule());
        prepareForJobGenRewrites.add(new SetExecutionModeRule());
        return prepareForJobGenRewrites;
    }
}
