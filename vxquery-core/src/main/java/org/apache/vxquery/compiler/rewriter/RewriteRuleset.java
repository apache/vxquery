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

import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.BreakSelectIntoConjunctsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexJoinInferenceRule;
import org.apache.hyracks.algebricks.rewriter.rules.ComplexUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateAssignsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ConsolidateSelectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.CopyLimitDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.EliminateGroupByEmptyKeyRule;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractCommonOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractGbyExpressionsRule;
import org.apache.hyracks.algebricks.rewriter.rules.FactorRedundantGroupAndDecorVarsRule;
import org.apache.hyracks.algebricks.rewriter.rules.InferTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineAssignIntoAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroJoinInsideSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceAggregateCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceGroupByCombinerRule;
import org.apache.hyracks.algebricks.rewriter.rules.IntroduceProjectsRule;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;
import org.apache.hyracks.algebricks.rewriter.rules.PushMapOperatorDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushProjectIntoDataSourceScanRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectDownRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSelectIntoJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.PushSubplanWithAggregateDownThroughProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.ReinferAllTypesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.SetExecutionModeRule;
import org.apache.hyracks.algebricks.rewriter.rules.SimpleUnnestToProductRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.EliminateSubplanRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.EliminateSubplanWithInputCardinalityOneRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.NestedSubplanToJoinRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.PushSubplanIntoGroupByRule;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.SubplanOutOfGroupRule;
import org.apache.vxquery.compiler.rewriter.algebricks_new_version.NestGroupByRule;
import org.apache.vxquery.compiler.rewriter.algebricks_new_version.PushGroupByThroughProduct;
import org.apache.vxquery.compiler.rewriter.rules.ConsolidateAssignAggregateRule;
import org.apache.vxquery.compiler.rewriter.rules.ConsolidateDescandantChild;
import org.apache.vxquery.compiler.rewriter.rules.ConvertAssignToAggregateRule;
import org.apache.vxquery.compiler.rewriter.rules.ConvertAssignToUnnestRule;
import org.apache.vxquery.compiler.rewriter.rules.ConvertFromAlgebricksExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.ConvertToAlgebricksExpressionsRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateSubplanForSingleItemsRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateUnnestAggregateSequencesRule;
import org.apache.vxquery.compiler.rewriter.rules.EliminateUnnestAggregateSubplanRule;
import org.apache.vxquery.compiler.rewriter.rules.IntroduceCollectionRule;
import org.apache.vxquery.compiler.rewriter.rules.IntroduceIndexingRule;
import org.apache.vxquery.compiler.rewriter.rules.IntroduceTwoStepAggregateRule;
import org.apache.vxquery.compiler.rewriter.rules.PushAggregateIntoGroupbyRule;
import org.apache.vxquery.compiler.rewriter.rules.PushChildIntoDataScanRule;
import org.apache.vxquery.compiler.rewriter.rules.PushFunctionsOntoEqJoinBranches;
import org.apache.vxquery.compiler.rewriter.rules.PushKeysOrMembersIntoDatascanRule;
import org.apache.vxquery.compiler.rewriter.rules.PushValueIntoDatascanRule;
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

public class RewriteRuleset {
    RewriteRuleset() {
    }

    /**
     * Optimizations specific to XQuery.
     *
     * @return List of algebraic rewrite rules.
     */
    public static final List<IAlgebraicRewriteRule> buildPathStepNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<>();
        normalization.add(new SetVariableIdContextRule());
        normalization.add(new InferTypesRule());
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
        normalization.add(new IntroduceIndexingRule());

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
    public static final List<IAlgebraicRewriteRule> buildXQueryNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<>();

        // Find assign for scalar aggregate function followed by an aggregate operator.
        normalization.add(new ConsolidateAssignAggregateRule());
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());

        // Find assign for scalar aggregate function.
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
    public static final List<IAlgebraicRewriteRule> buildInlineRedundantExpressionNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<>();
        normalization.add(new InlineNestedVariablesRule());
        normalization.add(new RemoveRedundantTreatExpressionsRule());
        normalization.add(new RemoveRedundantDataExpressionsRule());
        normalization.add(new RemoveRedundantPromoteExpressionsRule());
        normalization.add(new RemoveRedundantCastExpressionsRule());
        normalization.add(new ConvertToAlgebricksExpressionsRule());
        normalization.add(new RemoveRedundantBooleanExpressionsRule());
        // Clean up
        normalization.add(new ConvertAssignToAggregateRule());
        normalization.add(new IntroduceTwoStepAggregateRule());
        normalization.add(new RemoveRedundantVariablesRule());
        normalization.add(new RemoveUnusedAssignAndAggregateRule());
        normalization.add(new PushValueIntoDatascanRule());
        normalization.add(new PushKeysOrMembersIntoDatascanRule());
        return normalization;
    }

    /**
     * When a nested data sources exist, convert the plan to use the join operator.
     *
     * @return List of algebraic rewrite rules.
     */
    public static final List<IAlgebraicRewriteRule> buildNestedDataSourceRuleCollection() {
        List<IAlgebraicRewriteRule> xquery = new LinkedList<>();
        xquery.add(new BreakSelectIntoConjunctsRule());
        xquery.add(new SimpleUnnestToProductRule());
        xquery.add(new PushMapOperatorDownThroughProductRule());
        xquery.add(new PushSubplanWithAggregateDownThroughProductRule());
        xquery.add(new PushMapOperatorDownThroughProductRule());
        xquery.add(new PushGroupByThroughProduct());
        xquery.add(new PushSelectDownRule());
        xquery.add(new PushSelectIntoJoinRule());

        // Clean up
        xquery.add(new RemoveRedundantVariablesRule());
        xquery.add(new RemoveUnusedAssignAndAggregateRule());
        return xquery;
    }

    public static final List<IAlgebraicRewriteRule> buildTypeInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> typeInfer = new LinkedList<>();
        typeInfer.add(new InferTypesRule());
        return typeInfer;
    }

    /**
     * Unnest more complex structures.
     *
     * @return List of algebraic rewrite rules.
     */
    public static final List<IAlgebraicRewriteRule> buildUnnestingRuleCollection() {
        List<IAlgebraicRewriteRule> xquery = new LinkedList<>();

        xquery.add(new PushSelectDownRule());
        xquery.add(new ComplexUnnestToProductRule());
        xquery.add(new ComplexJoinInferenceRule());
        xquery.add(new PushSelectIntoJoinRule());
        xquery.add(new IntroJoinInsideSubplanRule());
        xquery.add(new PushMapOperatorDownThroughProductRule());
        xquery.add(new PushSubplanWithAggregateDownThroughProductRule());
        //xquery.add(new IntroduceGroupByForSubplanRule());
        xquery.add(new SubplanOutOfGroupRule());
        //        xquery.add(new InsertOuterJoinRule());
        xquery.add(new ExtractFunctionsFromJoinConditionRule());
        xquery.add(new RemoveRedundantVariablesRule());
        xquery.add(new RemoveUnusedAssignAndAggregateRule());
        xquery.add(new FactorRedundantGroupAndDecorVarsRule());
        return xquery;
    }

    public static final List<IAlgebraicRewriteRule> buildNormalizationRuleCollection() {
        List<IAlgebraicRewriteRule> normalization = new LinkedList<>();
        normalization.add(new EliminateSubplanRule());
        normalization.add(new SimpleUnnestToProductRule());
        normalization.add(new NestedSubplanToJoinRule());
        normalization.add(new EliminateSubplanWithInputCardinalityOneRule());
        normalization.add(new BreakSelectIntoConjunctsRule());
        normalization.add(new PushSelectIntoJoinRule());
        normalization.add(new ExtractGbyExpressionsRule());
        return normalization;
    }

    public static final List<IAlgebraicRewriteRule> buildCondPushDownRuleCollection() {
        List<IAlgebraicRewriteRule> condPushDown = new LinkedList<>();
        condPushDown.add(new PushSelectDownRule());
        condPushDown.add(new InlineVariablesRule());
        condPushDown.add(new SubplanOutOfGroupRule());
        condPushDown.add(new RemoveRedundantVariablesRule());
        condPushDown.add(new RemoveUnusedAssignAndAggregateRule());
        condPushDown.add(new FactorRedundantGroupAndDecorVarsRule());
        condPushDown.add(new PushAggregateIntoGroupbyRule());
        condPushDown.add(new EliminateSubplanRule());
        condPushDown.add(new PushGroupByThroughProduct());
        condPushDown.add(new NestGroupByRule());
        condPushDown.add(new EliminateGroupByEmptyKeyRule());
        condPushDown.add(new PushSubplanIntoGroupByRule());
        condPushDown.add(new NestedSubplanToJoinRule());
        return condPushDown;
    }

    public static final List<IAlgebraicRewriteRule> buildJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> joinInference = new LinkedList<>();
        joinInference.add(new InlineVariablesRule());
        joinInference.add(new ComplexJoinInferenceRule());
        return joinInference;
    }

    public static final List<IAlgebraicRewriteRule> buildOpPushDownRuleCollection() {
        List<IAlgebraicRewriteRule> opPushDown = new LinkedList<>();
        opPushDown.add(new PushProjectDownRule());
        opPushDown.add(new PushSelectDownRule());
        return opPushDown;
    }

    public static final List<IAlgebraicRewriteRule> buildIntroduceProjectRuleCollection() {
        List<IAlgebraicRewriteRule> project = new LinkedList<>();
        project.add(new IntroduceProjectsRule());
        return project;
    }

    public static final List<IAlgebraicRewriteRule> buildDataExchangeRuleCollection() {
        List<IAlgebraicRewriteRule> dataExchange = new LinkedList<>();
        dataExchange.add(new SetExecutionModeRule());
        return dataExchange;
    }

    public static final List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> consolidation = new LinkedList<>();
        consolidation.add(new ConsolidateSelectsRule());
        consolidation.add(new ConsolidateAssignsRule());
        consolidation.add(new InlineAssignIntoAggregateRule());
        consolidation.add(new IntroduceGroupByCombinerRule());
        consolidation.add(new IntroduceAggregateCombinerRule());
        consolidation.add(new RemoveUnusedAssignAndAggregateRule());
        return consolidation;
    }

    public static final List<IAlgebraicRewriteRule> buildPhysicalRewritesAllLevelsRuleCollection() {
        List<IAlgebraicRewriteRule> physicalPlanRewrites = new LinkedList<>();
        physicalPlanRewrites.add(new PullSelectOutOfEqJoin());
        physicalPlanRewrites.add(new PushFunctionsOntoEqJoinBranches());
        physicalPlanRewrites.add(new SetAlgebricksPhysicalOperatorsRule());
        physicalPlanRewrites.add(new SetExecutionModeRule());
        physicalPlanRewrites.add(new EnforceStructuralPropertiesRule());
        physicalPlanRewrites.add(new PushProjectDownRule());
        physicalPlanRewrites.add(new CopyLimitDownRule());
        return physicalPlanRewrites;
    }

    public static final List<IAlgebraicRewriteRule> buildPhysicalRewritesTopLevelRuleCollection() {
        List<IAlgebraicRewriteRule> physicalPlanRewrites = new LinkedList<>();
        physicalPlanRewrites.add(new CopyLimitDownRule());
        physicalPlanRewrites.add(new SetExecutionModeRule());
        return physicalPlanRewrites;
    }

    public static final List<IAlgebraicRewriteRule> prepareForJobGenRuleCollection() {
        List<IAlgebraicRewriteRule> prepareForJobGenRewrites = new LinkedList<>();
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
