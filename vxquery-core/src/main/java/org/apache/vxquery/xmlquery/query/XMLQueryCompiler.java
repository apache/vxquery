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
package org.apache.vxquery.xmlquery.query;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryBinaryBooleanInspectorFactory;
import org.apache.vxquery.compiler.algebricks.VXQueryBinaryIntegerInspectorFactory;
import org.apache.vxquery.compiler.algebricks.VXQueryComparatorFactoryProvider;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.algebricks.VXQueryExpressionRuntimeProvider;
import org.apache.vxquery.compiler.algebricks.VXQueryNullWriterFactory;
import org.apache.vxquery.compiler.algebricks.VXQueryPrinterFactoryProvider;
import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.compiler.rewriter.RewriteRuleset;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.runtime.provider.VXQueryBinaryHashFunctionFactoryProvider;
import org.apache.vxquery.runtime.provider.VXQueryBinaryHashFunctionFamilyProvider;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.translator.XMLQueryTranslator;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompiler;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompilerFactory;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.PrioritizedRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class XMLQueryCompiler {
    private final XQueryCompilationListener listener;

    private final ICompilerFactory cFactory;

    private LogicalOperatorPrettyPrintVisitor pprinter;

    private ModuleNode moduleNode;

    private Module module;

    private ICompiler compiler;

    private int frameSize;

    private String[] nodeList;

    public XMLQueryCompiler(XQueryCompilationListener listener, String[] nodeList, int frameSize) {
        this(listener, nodeList, frameSize, -1, -1, -1);
    }

    public XMLQueryCompiler(XQueryCompilationListener listener, String[] nodeList, int frameSize,
            int availableProcessors, long joinHashSize, long maximumDataSize) {
        this.listener = listener == null ? NoopXQueryCompilationListener.INSTANCE : listener;
        this.frameSize = frameSize;
        this.nodeList = nodeList;
        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder(
                new IOptimizationContextFactory() {
                    @Override
                    public IOptimizationContext createOptimizationContext(int varCounter,
                            IExpressionEvalSizeComputer expressionEvalSizeComputer,
                            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                            PhysicalOptimizationConfig physicalOptimizationConfig) {
                        return new VXQueryOptimizationContext(varCounter, expressionEvalSizeComputer,
                                mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                                physicalOptimizationConfig, pprinter);
                    }
                });
        builder.getPhysicalOptimizationConfig().setFrameSize(this.frameSize);
        if (joinHashSize > 0) {
            builder.getPhysicalOptimizationConfig().setMaxFramesHybridHash((int) (joinHashSize / this.frameSize));
        }
        if (maximumDataSize > 0) {
            builder.getPhysicalOptimizationConfig().setMaxFramesLeftInputHybridHash(
                    (int) (maximumDataSize / this.frameSize));
        }

        builder.getPhysicalOptimizationConfig().setMaxFramesLeftInputHybridHash(
                (int) (60L * 1024 * 1048576 / this.frameSize));

        builder.setLogicalRewrites(buildDefaultLogicalRewrites());
        builder.setPhysicalRewrites(buildDefaultPhysicalRewrites());
        builder.setSerializerDeserializerProvider(new ISerializerDeserializerProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public ISerializerDeserializer getSerializerDeserializer(Object type) throws AlgebricksException {
                return null;
            }
        });
        builder.setHashFunctionFactoryProvider(VXQueryBinaryHashFunctionFactoryProvider.INSTANCE);
        builder.setHashFunctionFamilyProvider(VXQueryBinaryHashFunctionFamilyProvider.INSTANCE);
        builder.setTypeTraitProvider(new ITypeTraitProvider() {
            @Override
            public ITypeTraits getTypeTrait(Object type) {
                return VoidPointable.TYPE_TRAITS;
            }
        });
        builder.setPrinterProvider(VXQueryPrinterFactoryProvider.INSTANCE);
        builder.setExpressionRuntimeProvider(new VXQueryExpressionRuntimeProvider());
        builder.setComparatorFactoryProvider(new VXQueryComparatorFactoryProvider());
        builder.setBinaryBooleanInspectorFactory(new VXQueryBinaryBooleanInspectorFactory());
        builder.setBinaryIntegerInspectorFactory(new VXQueryBinaryIntegerInspectorFactory());
        builder.setExpressionTypeComputer(new IExpressionTypeComputer() {
            @Override
            public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
                    IVariableTypeEnvironment env) throws AlgebricksException {
                if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression ce = (ConstantExpression) expr;
                    IAlgebricksConstantValue acv = ce.getValue();
                    if (acv == ConstantExpression.TRUE.getValue() || acv == ConstantExpression.FALSE.getValue()) {
                        return SequenceType.create(BuiltinTypeRegistry.XS_BOOLEAN, Quantifier.QUANT_ONE);
                    }
                    VXQueryConstantValue cv = (VXQueryConstantValue) acv;
                    return cv.getType();
                }
                return null;
            }
        });
        builder.setNullableTypeComputer(new INullableTypeComputer() {
            @Override
            public Object makeNullableType(Object type) throws AlgebricksException {
                throw new NotImplementedException("NullableTypeComputer is not implented (makeNullableType)");
            }

            @Override
            public boolean canBeNull(Object type) {
                return false;
            }

            @Override
            public Object getNonOptionalType(Object type) {
                throw new NotImplementedException("NullableTypeComputer is not implented (getNonOptionalType)");
            }
        });
        builder.setNullWriterFactory(new VXQueryNullWriterFactory());
        if (availableProcessors < 1) {
            builder.setClusterLocations(VXQueryMetadataProvider.getClusterLocations(nodeList));
        } else {
            builder.setClusterLocations(VXQueryMetadataProvider.getClusterLocations(nodeList, availableProcessors));
        }
        cFactory = builder.create();
    }

    public void compile(String name, Reader query, CompilerControlBlock ccb, int optimizationLevel)
            throws SystemException {
        moduleNode = XMLQueryParser.parse(name, query);
        listener.notifyParseResult(moduleNode);
        module = new XMLQueryTranslator(ccb).translateModule(moduleNode);
        pprinter = new LogicalOperatorPrettyPrintVisitor(new VXQueryLogicalExpressionPrettyPrintVisitor(
                module.getModuleContext()));
        VXQueryMetadataProvider mdProvider = new VXQueryMetadataProvider(nodeList, ccb.getSourceFileMap());
        compiler = cFactory.createCompiler(module.getBody(), mdProvider, 0);
        listener.notifyTranslationResult(module);
        XMLQueryTypeChecker.typeCheckModule(module);
        listener.notifyTypecheckResult(module);
        try {
            compiler.optimize();
        } catch (AlgebricksException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
        listener.notifyOptimizedResult(module);
        JobSpecification jobSpec;
        try {
            jobSpec = compiler.createJob(null, null);
            jobSpec.setFrameSize(frameSize);
        } catch (AlgebricksException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
        module.setHyracksJobSpecification(jobSpec);
        listener.notifyCodegenResult(module);
    }

    public ModuleNode getModuleNode() {
        return moduleNode;
    }

    public Module getModule() {
        return module;
    }

    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultLogicalRewrites() {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultLogicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(true);
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        PrioritizedRuleController priorityCtrl = new PrioritizedRuleController();
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(priorityCtrl,
                RewriteRuleset.buildPathStepNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(priorityCtrl,
                RewriteRuleset.buildXQueryNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RewriteRuleset.buildInlineRedundantExpressionNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(priorityCtrl,
                RewriteRuleset.buildNestedDataSourceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RewriteRuleset.buildTypeInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RewriteRuleset.buildNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RewriteRuleset.buildCondPushDownRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RewriteRuleset.buildJoinInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RewriteRuleset.buildOpPushDownRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RewriteRuleset.buildDataExchangeRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RewriteRuleset.buildConsolidationRuleCollection()));
        return defaultLogicalRewrites;
    }

    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultPhysicalRewrites() {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultPhysicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
        SequentialOnceRuleController seqOnceCtrlAllLevels = new SequentialOnceRuleController(true);
        SequentialOnceRuleController seqOnceCtrlTopLevel = new SequentialOnceRuleController(false);
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrlAllLevels,
                RewriteRuleset.buildPhysicalRewritesAllLevelsRuleCollection()));
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrlTopLevel,
                RewriteRuleset.buildPhysicalRewritesTopLevelRuleCollection()));
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrlAllLevels,
                RewriteRuleset.prepareForJobGenRuleCollection()));
        return defaultPhysicalRewrites;
    }

}