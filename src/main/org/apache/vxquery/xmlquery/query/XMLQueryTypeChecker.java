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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.expression.PINodeConstructorExpression;
import org.apache.vxquery.compiler.expression.PathStepExpression;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.expression.VariableReferenceExpression;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.functions.UserDefinedXQueryFunction;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.AnyNodeType;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.CommentType;
import org.apache.vxquery.types.DocumentType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.ItemType;
import org.apache.vxquery.types.ProcessingInstructionType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.TextType;
import org.apache.vxquery.types.TypeOperations;
import org.apache.vxquery.types.XQType;

final class XMLQueryTypeChecker {
    private XMLQueryTypeChecker() {
    }

    static void typeCheckModule(Module module) {
        StaticContext sCtx = module.getModuleContext();
        for (Iterator<Function> i = sCtx.listFunctions(); i.hasNext();) {
            Function f = i.next();
            if (Function.FunctionTag.UDXQUERY.equals(f.getTag())) {
                UserDefinedXQueryFunction udf = (UserDefinedXQueryFunction) f;
                Expression body = udf.getBody();
                body.accept(new ExpressionTypeChecker());
            }
        }
        if (module.getBody() != null) {
            module.getBody().accept(new ExpressionTypeChecker());
        }
    }

    private static final class ExpressionTypeChecker implements ExpressionVisitor<XQType> {
        private Map<Variable, XQType> varTypeMap;

        public ExpressionTypeChecker() {
            varTypeMap = new HashMap<Variable, XQType>();
        }

        @Override
        public XQType visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr) {
            XQType nameType = expr.getName().accept(this);
            XQType contentType = expr.getContent().accept(this);

            ItemType riType = AttributeType.ANYATTRIBUTE;
            return riType;
        }

        @Override
        public XQType visitCastExpression(CastExpression expr) {
            XQType inType = expr.getInput().accept(this);
            XQType cType = expr.getType().toXQType();

            return cType;
        }

        @Override
        public XQType visitCastableExpression(CastableExpression expr) {
            expr.getInput().accept(this);
            return BuiltinTypeRegistry.XS_BOOLEAN;
        }

        @Override
        public XQType visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr) {
            expr.getContent().accept(this);
            return CommentType.INSTANCE;
        }

        @Override
        public XQType visitConstantExpression(ConstantExpression expr) {
            return expr.getType().toXQType();
        }

        @Override
        public XQType visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr) {
            expr.getContent().accept(this);
            return DocumentType.ANYDOCUMENT;
        }

        @Override
        public XQType visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr) {
            expr.getName().accept(this);
            expr.getContent().accept(this);
            return ElementType.ANYELEMENT;
        }

        @Override
        public XQType visitExtensionExpression(ExtensionExpression expr) {
            return expr.getInput().accept(this);
        }

        @Override
        public XQType visitFLWORExpression(FLWORExpression expr) {
            Quantifier quant = Quantifier.QUANT_ONE;
            for (FLWORExpression.Clause clause : expr.getClauses()) {
                switch (clause.getTag()) {
                    case FOR: {
                        FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                        ForLetVariable fVar = fc.getForVariable();
                        XQType sType = fVar.getSequence().accept(this);
                        XQType varType = TypeOperations.primeType(sType);
                        quant = Quantifier.product(quant, TypeOperations.quantifier(sType));
                        varTypeMap.put(fVar, varType);
                        if (fc.getPosVariable() != null) {
                            varTypeMap.put(fc.getPosVariable(), BuiltinTypeRegistry.XS_INTEGER);
                        }
                        break;
                    }
                    case LET: {
                        FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                        ForLetVariable lVar = lc.getLetVariable();
                        XQType sType = lVar.getSequence().accept(this);
                        varTypeMap.put(lVar, sType);
                        break;
                    }
                    case ORDERBY: {
                        FLWORExpression.OrderbyClause oc = (FLWORExpression.OrderbyClause) clause;
                        for (ExpressionHandle e : oc.getOrderingExpressions()) {
                            XQType eType = e.get().accept(this);
                        }
                        break;
                    }
                    case WHERE: {
                        FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                        XQType cType = wc.getCondition().accept(this);
                        quant = Quantifier.product(quant, Quantifier.QUANT_QUESTION);
                        break;
                    }
                }
            }
            XQType rType = expr.getReturnExpression().accept(this);
            return TypeOperations.quantified(rType, quant);
        }

        @Override
        public XQType visitFunctionCallExpression(FunctionCallExpression expr) {
            for (ExpressionHandle arg : expr.getArguments()) {
                arg.get().accept(this);
            }
            return expr.getFunction().getSignature().getReturnType().toXQType();
        }

        @Override
        public XQType visitIfThenElseExpression(IfThenElseExpression expr) {
            XQType cType = expr.getCondition().accept(this);
            XQType tType = expr.getThenExpression().accept(this);
            XQType eType = expr.getElseExpression().accept(this);
            return TypeOperations.union(tType, eType);
        }

        @Override
        public XQType visitInstanceofExpression(InstanceofExpression expr) {
            expr.getInput().accept(this);
            return BuiltinTypeRegistry.XS_BOOLEAN;
        }

        @Override
        public XQType visitPINodeConstructorExpression(PINodeConstructorExpression expr) {
            expr.getTarget().accept(this);
            expr.getContent().accept(this);
            return ProcessingInstructionType.ANYPI;
        }

        @Override
        public XQType visitPromoteExpression(PromoteExpression expr) {
            XQType iType = expr.getInput().accept(this);
            return expr.getType().toXQType();
        }

        @Override
        public XQType visitQuantifiedExpression(QuantifiedExpression expr) {
            for (ForLetVariable var : expr.getQuantifiedVariables()) {
                XQType sType = var.getSequence().accept(this);
                varTypeMap.put(var, TypeOperations.primeType(sType));
            }
            XQType seType = expr.getSatisfiesExpression().accept(this);
            return BuiltinTypeRegistry.XS_BOOLEAN;
        }

        @Override
        public XQType visitTextNodeConstructorExpression(TextNodeConstructorExpression expr) {
            expr.getContent().accept(this);
            return TextType.INSTANCE;
        }

        @Override
        public XQType visitTreatExpression(TreatExpression expr) {
            XQType iType = expr.getInput().accept(this);
            return expr.getType().toXQType();
        }

        @Override
        public XQType visitTypeswitchExpression(TypeswitchExpression expr) {
            return null;
        }

        @Override
        public XQType visitValidateExpression(ValidateExpression expr) {
            expr.getInput().accept(this);
            return TypeOperations.quantified(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
        }

        @Override
        public XQType visitVariableReferenceExpression(VariableReferenceExpression expr) {
            Variable var = expr.getVariable();
            return varTypeMap.get(var);
        }

        @Override
        public XQType visitPathStepExpression(PathStepExpression expr) {
            XQType iType = expr.getInput().accept(this);
            return AnyNodeType.INSTANCE;
        }
    }
}