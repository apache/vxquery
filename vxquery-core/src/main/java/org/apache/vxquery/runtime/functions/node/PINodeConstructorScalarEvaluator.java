package org.apache.vxquery.runtime.functions.node;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.builders.nodes.PINodeBuilder;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class PINodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final PINodeBuilder pinb;

    private final VoidPointable vp;

    public PINodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        pinb = new PINodeBuilder();
        vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        pinb.reset(mvs);
        TaggedValuePointable targetArg = args[0];
        targetArg.getValue(vp);
        pinb.setTarget(vp);
        TaggedValuePointable contentArg = args[1];
        contentArg.getValue(vp);
        pinb.setContent(vp);
        pinb.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return false;
    }
}