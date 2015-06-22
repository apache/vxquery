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
package org.apache.vxquery.compiler.rewriter.rules;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.atomic.StringValueBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

/**
 * The rule searches for where the function_doc1 function is in the plan in place of XQuery function.
 * It replaces the string contained in the function with its absolute file path.
 * 
 * <pre>
 * Before
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : function_doc1( \@string ) )
 *   plan__child
 *   
 *   Where xquery_function creates an atomic value.
 *   
 * After 
 * 
 *   plan__parent
 *   %OPERATOR( $v1 : function_doc1( \@absolute_file_path ) ) )
 *   plan__child
 * </pre>
 * 
 * @author shivanim
 */

public class ConvertDocExpressionToFile implements IAlgebraicRewriteRule {

    final ByteBufferInputStream bbis = new ByteBufferInputStream();
    final DataInputStream di = new DataInputStream(bbis);
    final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    final DataOutput dOut = abvs.getDataOutput();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;
        //returns the list of expressions inside the operator.
        List<Mutable<ILogicalExpression>> expressions = OperatorToolbox.getExpressions(opRef);
        //for each expression we go in
        for (Mutable<ILogicalExpression> expression : expressions) {
            //checks if the expression is a function call
            //checks if the function call is fn_doc1
            //returns the first expression contained in it only!
            //what is a function has multiple arguments that is multiple expressions
            Mutable<ILogicalExpression> docExpression = ExpressionToolbox.findFirstFunctionExpression(expression,
                    BuiltinFunctions.FN_DOC_1.getFunctionIdentifier());
            if (docExpression != null) {
                AbstractFunctionCallExpression absFnCall = (AbstractFunctionCallExpression) docExpression.getValue();
                if (docExpression != null && ifDocExpressionFound(opRef, absFnCall.getArguments().get(0), context)) {
                    modified = true;
                }
            }
        }
        return modified;
    }

    //side note: I only see nested arguments, not multiple expressions in most cases.//
    //Expressions == arguments ??

    protected boolean ifDocExpressionFound(Mutable<ILogicalOperator> opRef, Mutable<ILogicalExpression> funcExpression,
            IOptimizationContext context) {
        VXQueryConstantValue constantValue = null;
        ConstantExpression constantExpression = null;
        ILogicalExpression logicalExpression = (ILogicalExpression) funcExpression.getValue();

        if (logicalExpression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {

            constantExpression = (ConstantExpression) logicalExpression;
            constantValue = (VXQueryConstantValue) constantExpression.getValue();
        } else if (logicalExpression.getExpressionTag() == LogicalExpressionTag.VARIABLE) {

            VariableReferenceExpression varLogicalExpression = (VariableReferenceExpression) logicalExpression;
            Mutable<ILogicalOperator> lop = OperatorToolbox.findProducerOf(opRef,
                    varLogicalExpression.getVariableReference());
            ILogicalExpression variableLogicalExpression = (ILogicalExpression) OperatorToolbox.getExpressionOf(lop,
                    varLogicalExpression.getVariableReference()).getValue();
            if (variableLogicalExpression.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return false;
            }
            constantExpression = (ConstantExpression) variableLogicalExpression;
            constantValue = (VXQueryConstantValue) constantExpression.getValue();
        } else {
            return false;
        }

        if (constantValue.getType() != SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)) {
            return false;
        }
        tvp.set(constantValue.getValue(), 0, constantValue.getValue().length);
        String collectionName = null;
        tvp.getValue(stringp);
        if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
            return false;
        }
        try {
            bbis.setByteBuffer(
                    ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(), stringp.getStartOffset(),
                            stringp.getLength() + stringp.getStartOffset())), 0);
            collectionName = di.readUTF();
        } catch (IOException e) {
            e.printStackTrace();
        }
        VXQueryMetadataProvider mdp = (VXQueryMetadataProvider) context.getMetadataProvider();
        if (!mdp.sourceFileMap.containsKey(collectionName)) {
            return false;
        }
        File file = mdp.sourceFileMap.get(collectionName);
        StringValueBuilder svb = new StringValueBuilder();
        try {
            abvs.reset();
            dOut.write(ValueTag.XS_STRING_TAG);
            svb.write(file.getAbsolutePath(), dOut);

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        VXQueryConstantValue vxqcv = new VXQueryConstantValue(SequenceType.create(BuiltinTypeRegistry.XS_STRING,
                Quantifier.QUANT_ONE), abvs.getByteArray());
        constantExpression.setValue(vxqcv);
        return true;
    }
}