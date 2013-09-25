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

import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.ExprTag;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionVisitor;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.XDMAtomicValue;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.BooleanValue;
import org.apache.vxquery.types.BuiltinTypeConstants;
import org.apache.vxquery.types.XQType;

public final class ExpressionUtils {
    private ExpressionUtils() {
    }

    public static void findFreeVariables(Expression e, Set<Variable> vars) {
        e.accept(new FreeVariableFinder(vars));
    }

    public static FreeVariableMaintainer createFreeVariableMaintainer(Set<Variable> freeVars) {
        return new FreeVariableMaintainer(freeVars);
    }

    public static QName createVariableCopyName(QName vName) {
        return new QName(vName.getNamespaceURI(), vName.getLocalPart() + "#", vName.getPrefix());
    }

    public static ExpressionVisitor<XQType> createTypeInferringVisitor() {
        return ExpressionTypeInferringVisitor.INSTANCE;
    }

    public static Boolean getBooleanValue(Expression expr) {
        if (expr.getTag() == ExprTag.CONSTANT) {
            ConstantExpression ce = (ConstantExpression) expr;
            XDMValue v = (XDMValue) ce.getValue();
            if (v.getDMOKind() == DMOKind.ATOMIC_VALUE) {
                XDMAtomicValue av = (XDMAtomicValue) v;
                if (av.getAtomicType().getTypeId() == BuiltinTypeConstants.XS_BOOLEAN_TYPE_ID) {
                    BooleanValue bv = (BooleanValue) av;
                    return bv.getBooleanValue();
                }
            }
        }
        return null;
    }
}