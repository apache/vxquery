package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class FnLowerCaseEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnLowerCaseEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        return new AbstractStringTranscodingFunction(args) {
            protected char transcodeCharacter(char c) {
                return Character.toLowerCase(c);
            }
        };
    }
}
