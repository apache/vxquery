package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToNonPositiveIntegerOperation extends CastToIntegerOperation {

    public CastToNonPositiveIntegerOperation() {
        negativeAllowed = true;
        negativeRequired = true;
        returnTag = ValueTag.XS_NON_NEGATIVE_INTEGER_TAG;
    }

}