package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToNonNegativeIntegerOperation extends CastToIntegerOperation {

    public CastToNonNegativeIntegerOperation() {
        negativeAllowed = true;
        negativeRequired = true;
        returnTag = ValueTag.XS_NON_POSITIVE_INTEGER_TAG;
    }

}