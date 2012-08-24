package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToNegativeIntegerOperation extends CastToIntegerOperation {

    public CastToNegativeIntegerOperation() {
        negativeAllowed = true;
        negativeRequired = true;
        returnTag = ValueTag.XS_NON_POSITIVE_INTEGER_TAG;
    }

}