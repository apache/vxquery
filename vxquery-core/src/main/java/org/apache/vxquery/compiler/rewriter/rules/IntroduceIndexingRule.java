package org.apache.vxquery.compiler.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.vxquery.compiler.rewriter.VXQueryOptimizationContext;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.metadata.VXQueryIndexingDatasource;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

/**
 *
 */
public class IntroduceIndexingRule extends AbstractCollectionRule {
    private String collection;
    private String index;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        VXQueryOptimizationContext vxqueryContext = (VXQueryOptimizationContext) context;
        String args[] = getFunctionalArguments(opRef, BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_2, BuiltinFunctions
                .FN_UPDATE_INDEX_1, BuiltinFunctions.FN_DELETE_INDEX_1);


        if (args != null) {

            // Check if the function call is for build-collection-on-index.
            // In build-collection-on-index, args[0] contains collection and args[1] contains index.
            // In all other queries, args[0] contains index.
            if (args.length == 2) {
                collection = args[0];
                index = args[1];
            } else {
                index = args[0];
            }

            // Build the new operator and update the query plan.
            int collectionId = vxqueryContext.newCollectionId();
            VXQueryIndexingDatasource ids = VXQueryIndexingDatasource.create(collectionId,
                     index, collection, SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR));
            if (ids != null) {
                ids.setTotalDataSources(vxqueryContext.getTotalDataSources());

                // Check if the call is for build-index-on-collection
                if (args.length == 2) {
                    ids.setTotalDataSources(vxqueryContext.getTotalDataSources());
                    ids.setTag(args[1]);
                }

                // Known to be true because of collection name.
                AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
                UnnestOperator unnest = (UnnestOperator) op;
                Mutable<ILogicalOperator> opRef2 = unnest.getInputs().get(0);
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
                AssignOperator assign = (AssignOperator) op2;

                DataSourceScanOperator opNew = new DataSourceScanOperator(assign.getVariables(), ids);
                opNew.getInputs().addAll(assign.getInputs());
                opRef2.setValue(opNew);
                return true;
            }
        }
        return false;
    }
}
