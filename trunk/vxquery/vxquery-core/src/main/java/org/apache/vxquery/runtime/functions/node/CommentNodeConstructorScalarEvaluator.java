package org.apache.vxquery.runtime.functions.node;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.nodes.CommentNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class CommentNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final CommentNodeBuilder cnb;

    private final VoidPointable vp;

    public CommentNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        cnb = new CommentNodeBuilder();
        vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        cnb.reset(mvs);
        TaggedValuePointable arg = args[0];
        if (arg.getTag() != ValueTag.XS_UNTYPED_ATOMIC_TAG && arg.getTag() != ValueTag.XS_STRING_TAG) {
            throw new SystemException(ErrorCode.TODO);
        }
        arg.getValue(vp);
        cnb.setValue(vp);
        cnb.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return false;
    }
}