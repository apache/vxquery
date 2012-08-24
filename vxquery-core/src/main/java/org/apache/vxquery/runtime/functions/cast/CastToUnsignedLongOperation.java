package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUnsignedLongOperation extends CastToIntegerOperation {

    public CastToUnsignedLongOperation() {
        negativeAllowed = false;
        returnTag = ValueTag.XS_UNSIGNED_LONG_TAG;
    }

}