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
package org.apache.vxquery.compiler.tools;

import org.apache.vxquery.compiler.expression.AttributeNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.CastExpression;
import org.apache.vxquery.compiler.expression.CastableExpression;
import org.apache.vxquery.compiler.expression.CommentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.DocumentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ElementNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.Expression;
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
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.expression.VariableReferenceExpression;
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

class ExpressionTypeInferringVisitor implements ExpressionVisitor<XQType> {
    static final ExpressionTypeInferringVisitor INSTANCE = new ExpressionTypeInferringVisitor();

    private ExpressionTypeInferringVisitor() {
    }

    @Override
    public XQType visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr) {
        ItemType riType = AttributeType.ANYATTRIBUTE;
        return riType;
    }

    @Override
    public XQType visitCastExpression(CastExpression expr) {
        XQType cType = expr.getType().toXQType();

        return cType;
    }

    @Override
    public XQType visitCastableExpression(CastableExpression expr) {
        return BuiltinTypeRegistry.XS_BOOLEAN;
    }

    @Override
    public XQType visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr) {
        return CommentType.INSTANCE;
    }

    @Override
    public XQType visitConstantExpression(ConstantExpression expr) {
        return expr.getType().toXQType();
    }

    @Override
    public XQType visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr) {
        return DocumentType.ANYDOCUMENT;
    }

    @Override
    public XQType visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr) {
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
                    quant = Quantifier.product(quant, TypeOperations.quantifier(sType));
                    break;
                }
                case WHERE: {
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
        return expr.getFunction().getSignature().getReturnType().toXQType();
    }

    @Override
    public XQType visitIfThenElseExpression(IfThenElseExpression expr) {
        XQType tType = expr.getThenExpression().accept(this);
        XQType eType = expr.getElseExpression().accept(this);
        return TypeOperations.union(tType, eType);
    }

    @Override
    public XQType visitInstanceofExpression(InstanceofExpression expr) {
        return BuiltinTypeRegistry.XS_BOOLEAN;
    }

    @Override
    public XQType visitPINodeConstructorExpression(PINodeConstructorExpression expr) {
        return ProcessingInstructionType.ANYPI;
    }

    @Override
    public XQType visitPromoteExpression(PromoteExpression expr) {
        return expr.getType().toXQType();
    }

    @Override
    public XQType visitQuantifiedExpression(QuantifiedExpression expr) {
        return BuiltinTypeRegistry.XS_BOOLEAN;
    }

    @Override
    public XQType visitTextNodeConstructorExpression(TextNodeConstructorExpression expr) {
        return TextType.INSTANCE;
    }

    @Override
    public XQType visitTreatExpression(TreatExpression expr) {
        return expr.getType().toXQType();
    }

    @Override
    public XQType visitTypeswitchExpression(TypeswitchExpression expr) {
        return null;
    }

    @Override
    public XQType visitValidateExpression(ValidateExpression expr) {
        return TypeOperations.quantified(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
    }

    @Override
    public XQType visitVariableReferenceExpression(VariableReferenceExpression expr) {
        Variable var = expr.getVariable();
        switch (var.getVariableTag()) {
            case FOR: {
                ForLetVariable fv = (ForLetVariable) var;
                XQType sType = fv.getSequence().accept(this);
                return TypeOperations.primeType(sType);
            }

            case LET: {
                ForLetVariable lv = (ForLetVariable) var;
                return lv.getSequence().accept(this);
            }

            case GLOBAL: {
                GlobalVariable gv = (GlobalVariable) var;
                Expression initializer = gv.getInitializerExpression();
                if (initializer != null) {
                    return initializer.accept(this);
                }
                return gv.getDeclaredStaticType().toXQType();
            }

            case PARAMETER: {
                ParameterVariable pv = (ParameterVariable) var;
                return pv.getDeclaredStaticType().toXQType();
            }

            case POSITION: {
                return BuiltinTypeRegistry.XS_INTEGER;
            }

            case SCORE: {
                return BuiltinTypeRegistry.XS_DOUBLE;
            }

            case UNRESOLVED: {
                return TypeOperations.quantified(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
            }
        }
        throw new IllegalArgumentException("Unknown variable tag: " + var.getVariableTag());
    }

    @Override
    public XQType visitPathStepExpression(PathStepExpression expr) {
        return AnyNodeType.INSTANCE;
    }
}