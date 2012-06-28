package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public abstract class AbstractCharacterIteratorCopyingEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractCharacterIteratorCopyingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

}
