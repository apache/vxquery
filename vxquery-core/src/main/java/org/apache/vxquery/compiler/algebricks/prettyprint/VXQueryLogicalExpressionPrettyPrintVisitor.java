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
package org.apache.vxquery.compiler.algebricks.prettyprint;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.serializer.XMLSerializer;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class VXQueryLogicalExpressionPrettyPrintVisitor implements ILogicalExpressionVisitor<String, Integer> {

    StaticContext ctx;
    IntegerPointable ip;
    TaggedValuePointable tvp;
    XMLSerializer serializer;
    ByteArrayOutputStream os;
    PrintStream ps;

    public VXQueryLogicalExpressionPrettyPrintVisitor(StaticContext ctx) {
        this.ctx = ctx;
        this.ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        this.tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        this.serializer = new XMLSerializer();
        this.os = new ByteArrayOutputStream();
        this.ps = new PrintStream(os);
    }

    @Override
    public String visitConstantExpression(ConstantExpression expr, Integer indent) throws AlgebricksException {
        IAlgebricksConstantValue value = expr.getValue();
        if (value instanceof VXQueryConstantValue) {
            VXQueryConstantValue vxqValue = (VXQueryConstantValue) value;
            tvp.set(vxqValue.getValue(), 0, vxqValue.getValue().length);
            serializer.printTaggedValuePointable(ps, tvp);
            try {
                return vxqValue.getType() + ": " + os.toString("UTF8");
            } catch (UnsupportedEncodingException e) {
                // print stack trace and return the default
                e.printStackTrace();
            } finally {
                ps.flush();
                os.reset();
            }
        }
        return value.toString();
    }

    @Override
    public String visitVariableReferenceExpression(VariableReferenceExpression expr, Integer indent)
            throws AlgebricksException {
        return expr.toString();
    }

    @Override
    public String visitAggregateFunctionCallExpression(AggregateFunctionCallExpression expr, Integer indent)
            throws AlgebricksException {
        return appendFunction(new StringBuilder(), expr, indent).toString();
    }

    @Override
    public String visitScalarFunctionCallExpression(ScalarFunctionCallExpression expr, Integer indent)
            throws AlgebricksException {
        return appendFunction(new StringBuilder(), expr, indent).toString();
    }

    @Override
    public String visitStatefulFunctionCallExpression(StatefulFunctionCallExpression expr, Integer indent)
            throws AlgebricksException {
        return appendFunction(new StringBuilder(), expr, indent).toString();
    }

    @Override
    public String visitUnnestingFunctionCallExpression(UnnestingFunctionCallExpression expr, Integer indent)
            throws AlgebricksException {
        return appendFunction(new StringBuilder(), expr, indent).toString();
    }

    protected boolean identifiesTypeOperator(FunctionIdentifier fi) {
        return BuiltinOperators.PROMOTE.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.TREAT.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.CAST.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.CASTABLE.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.INSTANCE_OF.getFunctionIdentifier().equals(fi);
    }

    protected boolean identifiesPathStep(FunctionIdentifier fi) {
        return BuiltinOperators.CHILD.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.ATTRIBUTE.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.ANCESTOR.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.ANCESTOR_OR_SELF.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.DESCENDANT.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.DESCENDANT_OR_SELF.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.PARENT.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.FOLLOWING.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.FOLLOWING_SIBLING.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.PRECEDING.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.PRECEDING_SIBLING.getFunctionIdentifier().equals(fi)
                || BuiltinOperators.SELF.getFunctionIdentifier().equals(fi);
    }

    protected StringBuilder appendFunction(StringBuilder sb, AbstractFunctionCallExpression expr, Integer indent)
            throws AlgebricksException {
        assert expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL;
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        if (identifiesTypeOperator(fi) || identifiesPathStep(fi)) {
            final ILogicalExpression typeEx = expr.getArguments().get(1).getValue();
            assert typeEx.getExpressionTag() == LogicalExpressionTag.CONSTANT;
            SequenceType type = getSequenceType((ConstantExpression) typeEx);
            sb.append(fi + " <" + type + ">, Args:");
            appendArgument(sb, expr.getArguments().get(0), indent + 2);
        } else {
            sb.append("function-call: " + fi + ", Args:");
            appendArguments(sb, expr.getArguments(), indent + 2);            
        }
        return sb;
    }

    protected StringBuilder appendArguments(StringBuilder sb, List<Mutable<ILogicalExpression>> args, Integer indent)
            throws AlgebricksException {
        sb.append("[\n");
        for (Mutable<ILogicalExpression> arg : args) {
            addIndent(sb, indent + 2).append(arg.getValue().accept(this, indent + 2)).append("\n");
        }
        return addIndent(sb, indent).append("]");
    }

    protected StringBuilder appendArgument(StringBuilder sb, Mutable<ILogicalExpression> arg, Integer indent)
            throws AlgebricksException {
        sb.append("[\n");
        addIndent(sb, indent + 2).append(arg.getValue().accept(this, indent + 2)).append("\n");
        return addIndent(sb, indent).append("]");
    }

    protected SequenceType getSequenceType(final ConstantExpression cTypeEx) {
        final VXQueryConstantValue typeCodeVal = (VXQueryConstantValue) cTypeEx.getValue();
        tvp.set(typeCodeVal.getValue(), 0, typeCodeVal.getValue().length);
        assert tvp.getTag() == ValueTag.XS_INT_TAG;
        tvp.getValue(ip);
        int typeCode = ip.getInteger();
        SequenceType type = ctx.lookupSequenceType(typeCode);
        return type;
    }

    protected static final StringBuilder addIndent(StringBuilder buffer, int level) {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
    }

}
