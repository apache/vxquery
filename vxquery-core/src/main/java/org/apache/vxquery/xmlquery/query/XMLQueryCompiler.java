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
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.algebricks.VXQueryExpressionJobGen;
import org.apache.vxquery.compiler.algebricks.VXQueryPrinterFactoryProvider;
import org.apache.vxquery.compiler.rewriter.RewriteRuleset;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.translator.XMLQueryTranslator;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompiler;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompilerFactory;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class XMLQueryCompiler {
    private final XQueryCompilationListener listener;

    private final ICompilerFactory cFactory;

    private final VXQueryMetadataProvider mdProvider;

    private ModuleNode moduleNode;

    private Module module;

    private ICompiler compiler;

    public XMLQueryCompiler(XQueryCompilationListener listener) {
        this.listener = listener == null ? NoopXQueryCompilationListener.INSTANCE : listener;
        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder();
        builder.setLogicalRewrites(buildDefaultLogicalRewrites());
        builder.setPhysicalRewrites(buildDefaultPhysicalRewrites());
        builder.setSerializerDeserializerProvider(new ISerializerDeserializerProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public ISerializerDeserializer getSerializerDeserializer(Object type) throws AlgebricksException {
                return null;
            }
        });
        builder.setPrinterProvider(VXQueryPrinterFactoryProvider.INSTANCE);
        builder.setExprJobGen(new VXQueryExpressionJobGen());
        builder.setExpressionTypeComputer(new IExpressionTypeComputer() {
            @Override
            public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
                    IVariableTypeEnvironment env) throws AlgebricksException {
                if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression ce = (ConstantExpression) expr;
                    VXQueryConstantValue cv = (VXQueryConstantValue) ce.getValue();
                    return cv.getType();
                }
                return null;
            }
        });
        cFactory = builder.create();
        mdProvider = new VXQueryMetadataProvider();
    }

    public void compile(String name, Reader query, CompilerControlBlock ccb, int optimizationLevel)
            throws SystemException {
        moduleNode = XMLQueryParser.parse(name, query);
        listener.notifyParseResult(moduleNode);
        module = new XMLQueryTranslator(ccb).translateModule(moduleNode);
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
            jobSpec = compiler.createJob(null);
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