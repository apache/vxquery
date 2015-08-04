package org.apache.vxquery.compiler.rewriter.rules;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.rewriter.rules.util.ExpressionToolbox;
import org.apache.vxquery.compiler.rewriter.rules.util.OperatorToolbox;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule searches for assign operator with an aggregate function expression
 * immediately following an aggregate operator with a sequence expression.
 * XQuery aggregate functions are implemented in both scalar (one XDM Instance
 * input as a sequence) and iterative (a stream of XDM Instances each one is
 * single object).
 * 
 * <pre>
 * Before 
 * 
 *   plan__parent
 *   UNNEST( $v2 : fn:child( args[$v1] ) )
 *   UNNEST( $v1 : fn:descandant( args[$v0] ) )
 *   plan__child
 *   
 *   
 * After
 * 
 *   plan__parent
 *   UNNEST( $v2 : fn:descandant( args[$v0] ) )
 *   plan__child
 * 
 * 
 * </pre>
 * 
 * @author shivanimall
 */

public class ConsolidateDescandantChild implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        boolean modified = false;
        ILogicalOperator lo = opRef.getValue();
        if (!(lo.getOperatorTag().equals(LogicalOperatorTag.UNNEST))) {
            return modified;
        }
        AbstractUnnestOperator auo = (AbstractUnnestOperator) lo;
        Mutable<ILogicalExpression> childExpression = ExpressionToolbox.findFirstFunctionExpression(
                auo.getExpressionRef(), BuiltinOperators.CHILD.getFunctionIdentifier());
        if (childExpression == null) {
            return modified;
        }
        AbstractFunctionCallExpression childFnCall = (AbstractFunctionCallExpression) childExpression.getValue();
        List<Mutable<ILogicalExpression>> list = (List<Mutable<ILogicalExpression>>) childFnCall.getArguments();
        Mutable<ILogicalExpression> mle = (Mutable<ILogicalExpression>) (list).get(0);
        ILogicalExpression le = mle.getValue();
        if (!(le.getExpressionTag().equals(LogicalExpressionTag.VARIABLE))) {
            return modified;
        }
        VariableReferenceExpression varLogicalExpression = (VariableReferenceExpression) le;
        Mutable<ILogicalOperator> lop = OperatorToolbox.findProducerOf(opRef,
                varLogicalExpression.getVariableReference());
        ILogicalOperator lop1 = lop.getValue();
        if (!(lop1.getOperatorTag().equals(LogicalOperatorTag.UNNEST))) {
            return modified;
        }
        if (OperatorToolbox.getExpressionOf(lop, varLogicalExpression.getVariableReference()) == null) {
            return modified;
        }
        ILogicalExpression variableLogicalExpression = (ILogicalExpression) OperatorToolbox.getExpressionOf(lop,
                varLogicalExpression.getVariableReference()).getValue();
        if (!(variableLogicalExpression.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL))) {
            return modified;
        }
        AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) variableLogicalExpression;
        if (!(afce.getFunctionIdentifier().equals(BuiltinOperators.DESCENDANT_OR_SELF.getFunctionIdentifier()))) {
            return modified;
        }
        if (changeChildToDesc(opRef, childFnCall, afce, context)) {
            modified = true;
        }
        return modified;
    }

    private boolean changeChildToDesc(Mutable<ILogicalOperator> opRef, AbstractFunctionCallExpression child,
            AbstractFunctionCallExpression desc, IOptimizationContext context) {

        child.setFunctionInfo(BuiltinOperators.DESCENDANT_OR_SELF);
        child.getArguments().get(0).setValue(desc.getArguments().get(0).getValue());
        //can be passed in as argument
        ILogicalOperator lo = opRef.getValue();
        Mutable<ILogicalOperator> mlistOfLo = lo.getInputs().get(0).getValue().getInputs().get(0);
        ILogicalOperator ilo = (ILogicalOperator) mlistOfLo.getValue();
        //List<Mutable<ILogicalOperator>> listOfLo1 = listOfLo.get(0).getValue().getInputs();
        lo.getInputs().get(0).setValue((ILogicalOperator) ilo);
        return true;
    }
}