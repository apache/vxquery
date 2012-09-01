package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToNonNegativeIntegerOperation extends CastToIntegerOperation {

    public CastToNonNegativeIntegerOperation() {
        negativeAllowed = false;
        returnTag = ValueTag.XS_NON_NEGATIVE_INTEGER_TAG;
    }

}