/*
 * Licensed to the Apache Software Foundation (ExpressionHandle under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (ExpressionHandle "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either handleess or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.expression;

public interface MutableExpressionVisitor<T> {
    T visitAttributeNodeConstructorExpression(ExpressionHandle handle);

    T visitCastableExpression(ExpressionHandle handle);

    T visitCastExpression(ExpressionHandle handle);

    T visitCommentNodeConstructorExpression(ExpressionHandle handle);

    T visitConstantExpression(ExpressionHandle handle);

    T visitDocumentNodeConstructorExpression(ExpressionHandle handle);

    T visitElementNodeConstructorExpression(ExpressionHandle handle);

    T visitExtensionExpression(ExpressionHandle handle);

    T visitFLWORExpression(ExpressionHandle handle);

    T visitFunctionCallExpression(ExpressionHandle handle);

    T visitIfThenElseExpression(ExpressionHandle handle);

    T visitInstanceofExpression(ExpressionHandle handle);

    T visitPINodeConstructorExpression(ExpressionHandle handle);

    T visitPromoteExpression(ExpressionHandle handle);

    T visitQuantifiedExpression(ExpressionHandle handle);

    T visitTextNodeConstructorExpression(ExpressionHandle handle);

    T visitTreatExpression(ExpressionHandle handle);

    T visitTypeswitchExpression(ExpressionHandle handle);

    T visitValidateExpression(ExpressionHandle handle);

    T visitVariableReferenceExpression(ExpressionHandle handle);

    T visitPathStepExpression(ExpressionHandle handle);
}