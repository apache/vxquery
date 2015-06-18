package org.apache.vxquery.runtime.functions.node;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public abstract class AbstractNodePositionalCheckEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {

    public AbstractNodePositionalCheckEvaluator(IScalarEvaluator[] args) {
        super(args);
    }

    private final NodeTreePointable ntp1 = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private final NodeTreePointable ntp2 = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    private final TypedPointables tp = new TypedPointables();

    @Override
    protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {

        parameterTypeCheck(args[0], ntp1);
        parameterTypeCheck(args[1], ntp2);

        if (nodeCompare(FunctionHelper.getLocalNodeId(args[0], tp), FunctionHelper.getLocalNodeId(args[1], tp))) {
            XDMConstants.setTrue(result);
        } else {
            XDMConstants.setFalse(result);
        }
    }

    //node passed in from argument
    protected void parameterTypeCheck(TaggedValuePointable node, NodeTreePointable ntp) throws SystemException {
        if (node.getTag() == ValueTag.NODE_TREE_TAG) {
            node.getValue(ntp);
            return;
        }
        throw new SystemException(ErrorCode.FORG0006);
    }

    abstract protected boolean nodeCompare(int firstId, int secondId);

}
