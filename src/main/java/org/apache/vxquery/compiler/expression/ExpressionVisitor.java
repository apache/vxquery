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
package org.apache.vxquery.compiler.expression;

public interface ExpressionVisitor<T> {
    T visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr);

    T visitCastableExpression(CastableExpression expr);

    T visitCastExpression(CastExpression expr);

    T visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr);

    T visitConstantExpression(ConstantExpression expr);

    T visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr);

    T visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr);

    T visitExtensionExpression(ExtensionExpression expr);

    T visitFLWORExpression(FLWORExpression expr);

    T visitFunctionCallExpression(FunctionCallExpression expr);

    T visitIfThenElseExpression(IfThenElseExpression expr);

    T visitInstanceofExpression(InstanceofExpression expr);

    T visitPINodeConstructorExpression(PINodeConstructorExpression expr);

    T visitPromoteExpression(PromoteExpression expr);

    T visitQuantifiedExpression(QuantifiedExpression expr);

    T visitTextNodeConstructorExpression(TextNodeConstructorExpression expr);

    T visitTreatExpression(TreatExpression expr);

    T visitTypeswitchExpression(TypeswitchExpression expr);

    T visitValidateExpression(ValidateExpression expr);

    T visitVariableReferenceExpression(VariableReferenceExpression expr);

    T visitPathStepExpression(PathStepExpression expr);
}