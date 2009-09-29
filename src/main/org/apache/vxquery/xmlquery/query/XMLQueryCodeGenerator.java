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
package org.apache.vxquery.xmlquery.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.expression.AttributeNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.CastExpression;
import org.apache.vxquery.compiler.expression.CastableExpression;
import org.apache.vxquery.compiler.expression.CommentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.DocumentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ElementNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionHandle;
import org.apache.vxquery.compiler.expression.ExpressionVisitor;
import org.apache.vxquery.compiler.expression.ExtensionExpression;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.ForLetVariable;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.expression.GlobalVariable;
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.expression.PINodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ParameterVariable;
import org.apache.vxquery.compiler.expression.PathStepExpression;
import org.apache.vxquery.compiler.expression.PositionVariable;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.expression.VariableReferenceExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression.Quantification;
import org.apache.vxquery.compiler.expression.Variable.VarTag;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.BooleanValue;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.functions.UserDefinedXQueryFunction;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimePlan;
import org.apache.vxquery.runtime.base.FunctionIteratorFactory;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.base.TupleIterator;
import org.apache.vxquery.runtime.core.FLWORIterator;
import org.apache.vxquery.runtime.core.FilterTupleIterator;
import org.apache.vxquery.runtime.core.ForTupleIterator;
import org.apache.vxquery.runtime.core.GlobalRegisterAccessIterator;
import org.apache.vxquery.runtime.core.IfThenElseIterator;
import org.apache.vxquery.runtime.core.LetTupleIterator;
import org.apache.vxquery.runtime.core.LocalRegisterAccessIterator;
import org.apache.vxquery.runtime.core.SingletonRuntimeIterator;
import org.apache.vxquery.runtime.core.SingletonTupleIterator;
import org.apache.vxquery.runtime.nodes.AttributeConstructorIterator;
import org.apache.vxquery.runtime.nodes.CommentConstructorIterator;
import org.apache.vxquery.runtime.nodes.DocumentConstructorIterator;
import org.apache.vxquery.runtime.nodes.ElementConstructorIterator;
import org.apache.vxquery.runtime.nodes.PIConstructorIterator;
import org.apache.vxquery.runtime.nodes.TextConstructorIterator;
import org.apache.vxquery.runtime.paths.AncestorAxisIterator;
import org.apache.vxquery.runtime.paths.AncestorOrSelfAxisIterator;
import org.apache.vxquery.runtime.paths.AttributeAxisIterator;
import org.apache.vxquery.runtime.paths.ChildAxisIterator;
import org.apache.vxquery.runtime.paths.DescendantAxisIterator;
import org.apache.vxquery.runtime.paths.DescendantOrSelfAxisIterator;
import org.apache.vxquery.runtime.paths.FollowingSiblingAxisIterator;
import org.apache.vxquery.runtime.paths.ParentAxisIterator;
import org.apache.vxquery.runtime.paths.PrecedingSiblingAxisIterator;
import org.apache.vxquery.runtime.paths.SelfAxisIterator;
import org.apache.vxquery.runtime.types.CastIterator;
import org.apache.vxquery.runtime.types.CastableIterator;
import org.apache.vxquery.runtime.types.InstanceOfIterator;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.types.XQType;
import org.apache.vxquery.types.processors.CastProcessor;
import org.apache.vxquery.util.Filter;

class XMLQueryCodeGenerator {
    public static void codegenModule(CompilerControlBlock ccb, Module module) {
        StaticContext sCtx = module.getModuleContext();
        List<PrologVariable> prologVars = new ArrayList<PrologVariable>();
        Map<Variable, Integer> gVarMap = new HashMap<Variable, Integer>();
        int reg = 0;
        for (Iterator<Variable> i = sCtx.listVariables(); i.hasNext();) {
            Variable v = i.next();
            gVarMap.put(v, reg++);
        }
        for (Iterator<Variable> i = sCtx.listVariables(); i.hasNext();) {
            GlobalVariable v = (GlobalVariable) i.next();
            RuntimePlan varPlan = null;
            Expression initExpr = v.getInitializerExpression();
            if (initExpr != null) {
                RegisterAllocator varPlanRegAllocator = new RegisterAllocator(0);
                Map<Variable, Integer> varPlanVarMap = new HashMap<Variable, Integer>(gVarMap);
                varPlan = createPlan(initExpr, varPlanRegAllocator, varPlanVarMap, ccb);
            }
            PrologVariable gVar = new PrologVariable(v, varPlan);
            prologVars.add(gVar);
        }
        module.setPrologVariables(prologVars.toArray(new PrologVariable[prologVars.size()]));

        for (Iterator<Function> i = sCtx.listFunctions(); i.hasNext();) {
            Function f = i.next();
            if (Function.FunctionTag.UDXQUERY.equals(f.getTag())) {
                UserDefinedXQueryFunction udf = (UserDefinedXQueryFunction) f;
                Expression fnExpr = udf.getBody();
                RegisterAllocator fnPlanRegAllocator = new RegisterAllocator(0);
                Map<Variable, Integer> fnPlanVarMap = new HashMap<Variable, Integer>(gVarMap);
                for (ParameterVariable pVar : udf.getParameters()) {
                    fnPlanVarMap.put(pVar, fnPlanRegAllocator.allocate(1));
                }
                RuntimePlan fnPlan = createPlan(fnExpr, fnPlanRegAllocator, fnPlanVarMap, ccb);
                udf.setRuntimePlan(fnPlan);
            }
        }
        Expression body = module.getBody();
        if (body != null) {
            RegisterAllocator planRegAllocator = new RegisterAllocator(0);
            Map<Variable, Integer> planVarMap = new HashMap<Variable, Integer>(gVarMap);
            RuntimePlan bodyPlan = createPlan(body, planRegAllocator, planVarMap, ccb);
            module.setBodyRuntimePlan(bodyPlan);
        }
    }

    private static RuntimePlan createPlan(Expression expr, RegisterAllocator rAllocator, Map<Variable, Integer> varMap,
            CompilerControlBlock ccb) {
        ExpressionCodeGenerator ecg = new ExpressionCodeGenerator(rAllocator, varMap, ccb);
        RuntimeIterator ri = expr.accept(ecg);
        return new RuntimePlan(ri, rAllocator.getAllocatedRegisterCount());
    }

    private static final class ExpressionCodeGenerator implements ExpressionVisitor<RuntimeIterator> {
        private final RegisterAllocator rAllocator;
        private final Map<Variable, Integer> varRegMap;
        private final CompilerControlBlock ccb;

        ExpressionCodeGenerator(RegisterAllocator rAllocator, Map<Variable, Integer> varRegMap, CompilerControlBlock ccb) {
            this.rAllocator = rAllocator;
            this.varRegMap = varRegMap;
            this.ccb = ccb;
        }

        @Override
        public RuntimeIterator visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr) {
            RuntimeIterator ni = expr.getName().accept(this);
            RuntimeIterator ci = expr.getContent().accept(this);
            return new AttributeConstructorIterator(rAllocator, ni, ci);
        }

        @Override
        public RuntimeIterator visitCastExpression(CastExpression expr) {
            RuntimeIterator input = expr.getInput().accept(this);
            SequenceType targetSequenceType = expr.getType();
            XQType type = targetSequenceType.toXQType();
            CastProcessor cp = type.getCastProcessor(BuiltinTypeRegistry.XS_ANY_ATOMIC);
            return new CastIterator(rAllocator, input, cp, expr.getStaticContext());
        }

        @Override
        public RuntimeIterator visitCastableExpression(CastableExpression expr) {
            RuntimeIterator input = expr.getInput().accept(this);
            SequenceType targetSequenceType = expr.getType();
            XQType type = targetSequenceType.toXQType();
            CastProcessor cp = type.getCastProcessor(BuiltinTypeRegistry.XS_ANY_ATOMIC);
            return new CastableIterator(rAllocator, input, cp, expr.getStaticContext());
        }

        @Override
        public RuntimeIterator visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr) {
            RuntimeIterator ci = expr.getContent().accept(this);
            return new CommentConstructorIterator(rAllocator, ci);
        }

        @Override
        public RuntimeIterator visitConstantExpression(ConstantExpression expr) {
            return new SingletonRuntimeIterator(rAllocator, expr.getValue());
        }

        @Override
        public RuntimeIterator visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr) {
            RuntimeIterator ci = expr.getContent().accept(this);
            return new DocumentConstructorIterator(rAllocator, ci);
        }

        @Override
        public RuntimeIterator visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr) {
            RuntimeIterator ni = expr.getName().accept(this);
            RuntimeIterator ci = expr.getContent().accept(this);
            return new ElementConstructorIterator(rAllocator, ni, ci);
        }

        @Override
        public RuntimeIterator visitExtensionExpression(ExtensionExpression expr) {
            return expr.getInput().accept(this);
        }

        @Override
        public RuntimeIterator visitFLWORExpression(FLWORExpression expr) {
            List<FLWORExpression.Clause> clauses = expr.getClauses();
            TupleIterator ti = new SingletonTupleIterator(rAllocator);
            for (FLWORExpression.Clause clause : clauses) {
                switch (clause.getTag()) {
                    case FOR: {
                        FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                        ForLetVariable fv = fc.getForVariable();
                        PositionVariable pv = fc.getPosVariable();
                        RuntimeIterator si = fv.getSequence().accept(this);
                        int fvAddr = rAllocator.allocate(1);
                        varRegMap.put(fv, fvAddr);
                        int pvAddr = -1;
                        if (pv != null) {
                            pvAddr = rAllocator.allocate(1);
                            varRegMap.put(pv, pvAddr);
                        }
                        ti = new ForTupleIterator(rAllocator, ti, si, fvAddr, pvAddr);
                        break;
                    }

                    case LET: {
                        FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                        ForLetVariable lv = lc.getLetVariable();
                        RuntimeIterator si = lv.getSequence().accept(this);
                        int lvAddr = rAllocator.allocate(1);
                        varRegMap.put(lv, lvAddr);
                        ti = new LetTupleIterator(rAllocator, ti, si, lvAddr);
                        break;
                    }

                    case WHERE: {
                        FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                        RuntimeIterator ci = wc.getCondition().accept(this);
                        ti = new FilterTupleIterator(rAllocator, ti, ci);
                        break;
                    }

                    case ORDERBY:
                }
            }
            RuntimeIterator ri = expr.getReturnExpression().accept(this);
            return new FLWORIterator(rAllocator, ti, ri);
        }

        @Override
        public RuntimeIterator visitFunctionCallExpression(FunctionCallExpression expr) {
            List<ExpressionHandle> argExprs = expr.getArguments();
            RuntimeIterator[] args = new RuntimeIterator[argExprs.size()];
            for (int i = 0; i < args.length; ++i) {
                args[i] = argExprs.get(i).get().accept(this);
            }
            Function fn = expr.getFunction();
            FunctionIteratorFactory fniFactory = fn.getIteratorFactory();
            if (fniFactory == null) {
                throw new UnsupportedOperationException("Unable to find FI factory for: " + fn.getName() + "/"
                        + fn.getSignature().getArity());
            }
            return fniFactory.createIterator(rAllocator, fn, args, expr.getStaticContext());
        }

        @Override
        public RuntimeIterator visitIfThenElseExpression(IfThenElseExpression expr) {
            RuntimeIterator ci = expr.getCondition().accept(this);
            RuntimeIterator ti = expr.getThenExpression().accept(this);
            RuntimeIterator ei = expr.getElseExpression().accept(this);
            return new IfThenElseIterator(rAllocator, ci, ti, ei);
        }

        @Override
        public RuntimeIterator visitInstanceofExpression(InstanceofExpression expr) {
            Filter<XDMValue> filter = expr.getType().toXQType().createInstanceOfFilter(ccb.getNameCache());
            RuntimeIterator ii = expr.getInput().accept(this);
            return new InstanceOfIterator(rAllocator, ii, filter);
        }

        @Override
        public RuntimeIterator visitPINodeConstructorExpression(PINodeConstructorExpression expr) {
            RuntimeIterator ni = expr.getTarget().accept(this);
            RuntimeIterator ci = expr.getContent().accept(this);
            return new PIConstructorIterator(rAllocator, ni, ci);
        }

        @Override
        public RuntimeIterator visitPromoteExpression(PromoteExpression expr) {
            return expr.getInput().accept(this);
        }

        @Override
        public RuntimeIterator visitQuantifiedExpression(QuantifiedExpression expr) {
            TupleIterator ti = new SingletonTupleIterator(rAllocator);
            for (ForLetVariable fv : expr.getQuantifiedVariables()) {
                RuntimeIterator si = fv.getSequence().accept(this);
                int rAddr = rAllocator.allocate(1);
                varRegMap.put(fv, rAddr);
                ti = new ForTupleIterator(rAllocator, ti, si, rAddr, -1);
            }
            Expression satExpr = expr.getSatisfiesExpression();
            RuntimeIterator ci = satExpr.accept(this);
            if (expr.getQuantification() == Quantification.EVERY) {
                ci = BuiltinFunctions.FN_NOT_1.getIteratorFactory().createIterator(rAllocator,
                        BuiltinFunctions.FN_NOT_1, new RuntimeIterator[] { ci }, satExpr.getStaticContext());
            }
            ti = new FilterTupleIterator(rAllocator, ti, ci);
            RuntimeIterator ri = new FLWORIterator(rAllocator, ti, new SingletonRuntimeIterator(rAllocator,
                    BooleanValue.TRUE));
            Function fn = expr.getQuantification() == Quantification.SOME ? BuiltinFunctions.FN_EXISTS_1
                    : BuiltinFunctions.FN_EMPTY_1;
            ri = fn.getIteratorFactory().createIterator(rAllocator, fn, new RuntimeIterator[] { ri },
                    satExpr.getStaticContext());
            return ri;
        }

        @Override
        public RuntimeIterator visitTextNodeConstructorExpression(TextNodeConstructorExpression expr) {
            RuntimeIterator ci = expr.getContent().accept(this);
            return new TextConstructorIterator(rAllocator, ci);
        }

        @Override
        public RuntimeIterator visitTreatExpression(TreatExpression expr) {
            return expr.getInput().accept(this);
        }

        @Override
        public RuntimeIterator visitTypeswitchExpression(TypeswitchExpression expr) {
            return null;
        }

        @Override
        public RuntimeIterator visitValidateExpression(ValidateExpression expr) {
            return null;
        }

        @Override
        public RuntimeIterator visitVariableReferenceExpression(VariableReferenceExpression expr) {
            Variable v = expr.getVariable();
            Integer regInt = varRegMap.get(v);
            if (regInt == null) {
                throw new IllegalStateException("Encountered unbound variable: " + v);
            }
            if (v.getVariableTag() == VarTag.GLOBAL) {
                return new GlobalRegisterAccessIterator(rAllocator, regInt);
            }
            return new LocalRegisterAccessIterator(rAllocator, regInt);
        }

        @Override
        public RuntimeIterator visitPathStepExpression(PathStepExpression expr) {
            RuntimeIterator in = expr.getInput().accept(this);
            Filter<XDMValue> typeFilter = expr.getNodeType().createInstanceOfFilter(ccb.getNameCache());
            switch (expr.getAxis()) {
                case ANCESTOR:
                    return new AncestorAxisIterator(rAllocator, in, typeFilter);

                case ANCESTOR_OR_SELF:
                    return new AncestorOrSelfAxisIterator(rAllocator, in, typeFilter);

                case ATTRIBUTE:
                    return new AttributeAxisIterator(rAllocator, in, typeFilter);

                case CHILD:
                    return new ChildAxisIterator(rAllocator, in, typeFilter);

                case DESCENDANT:
                    return new DescendantAxisIterator(rAllocator, in, typeFilter);

                case DESCENDANT_OR_SELF:
                    return new DescendantOrSelfAxisIterator(rAllocator, in, typeFilter);

                case FOLLOWING:
                    throw new UnsupportedOperationException();

                case FOLLOWING_SIBLING:
                    return new FollowingSiblingAxisIterator(rAllocator, in, typeFilter);

                case PARENT:
                    return new ParentAxisIterator(rAllocator, in, typeFilter);

                case PRECEDING:
                    throw new UnsupportedOperationException();

                case PRECEDING_SIBLING:
                    return new PrecedingSiblingAxisIterator(rAllocator, in, typeFilter);

                case SELF:
                    return new SelfAxisIterator(rAllocator, in, typeFilter);
            }
            return null;
        }
    }
}