package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToPositiveIntegerOperation extends CastToIntegerOperation {

    public CastToPositiveIntegerOperation() {
        negativeAllowed = false;
        returnTag = ValueTag.XS_POSITIVE_INTEGER_TAG;
    }

}