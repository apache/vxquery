package org.apache.vxquery.compiler.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.metadata.IVXQueryDataSource;
import org.apache.vxquery.metadata.VXQueryIndexingDataSource;
import org.apache.vxquery.metadata.VXQueryMetadataProvider;
import org.apache.vxquery.types.ElementType;

public class PushIndexingValueIntoDatascanRule extends AbstractUsedVariablesProcessingRule {
    StaticContext dCtx = null;

    @Override
    protected boolean processOperator(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = null;
        AbstractLogicalOperator op3 = null;

        if (dCtx == null) {
            VXQueryOptimizationContext vxqueryCtx = (VXQueryOptimizationContext) context;
            dCtx = ((VXQueryMetadataProvider) vxqueryCtx.getMetadataProvider()).getStaticContext();
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op1;

        op2 = (AbstractLogicalOperator) select.getInputs().get(0).getValue();

        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op2;

        op3 = (AbstractLogicalOperator) subplan.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        DataSourceScanOperator datascan = (DataSourceScanOperator) op3;

        if (!usedVariables.contains(datascan.getVariables())) {

            Mutable<ILogicalExpression> expressionRef = select.getCondition();
            ILogicalOperator op = subplan.getNestedPlans().get(0).getRoots().get(0).getValue();
            AggregateOperator aggregate = (AggregateOperator) op;
            Mutable<ILogicalExpression> expressionRefSub = aggregate.getExpressions().get(0);
            if (!(updateDataSource((IVXQueryDataSource) datascan.getDataSource(), expressionRef, expressionRefSub))) {
                return false;
            }

            return true;
        }
        return false;

    }

    private boolean updateDataSource(IVXQueryDataSource dataSource, Mutable<ILogicalExpression> expression,
            Mutable<ILogicalExpression> subExpression) {
        if (!dataSource.usingIndex()) {
            return false;
        }
        VXQueryIndexingDataSource ids = (VXQueryIndexingDataSource) dataSource;
        boolean added = false;
        List<Mutable<ILogicalExpression>> finds = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> children = new ArrayList<Mutable<ILogicalExpression>>();
        ExpressionToolbox.findAllFunctionExpressions(subExpression, BuiltinOperators.CHILD.getFunctionIdentifier(),
                children);
        ILogicalExpression le = expression.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            ILogicalExpression le2 = afce.getArguments().get(1).getValue();
            if (le2.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression afce2 = (AbstractFunctionCallExpression) le2;
                finds = afce2.getArguments();
            }
        }
        for (int i = children.size(); i > 0; --i) {
            int typeId = ExpressionToolbox.getTypeExpressionTypeArgument(children.get(i - 1));
            if (typeId > 0) {
                ElementType it = (ElementType) dCtx.lookupSequenceType(typeId).getItemType();
                ElementType et = ElementType.ANYELEMENT;

                if (it.getContentType().equals(et.getContentType())) {
                    for (int child : ids.getChildSeq()) {
                        ids.addIndexSeq(child);
                    }
                    ids.addIndexSeq(typeId);
                }
            }
        }
        int typeId2 = convertConstantToInteger(finds.get(0));
        if (typeId2 > 0) {
            ids.addIndexSeq(typeId2);
            added = true;
        }
        return added;
    }

    public int convertConstantToInteger(Mutable<ILogicalExpression> finds) {
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        ExpressionToolbox.getConstantAsPointable((ConstantExpression) finds.getValue(), tvp);

        IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        tvp.getValue(pTypeCode);
        int typeId = pTypeCode.getInteger();
        return typeId;
    }

}
