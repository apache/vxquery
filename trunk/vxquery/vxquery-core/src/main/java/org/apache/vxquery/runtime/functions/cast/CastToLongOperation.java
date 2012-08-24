package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToLongOperation extends CastToIntegerOperation {

    public CastToLongOperation() {
        negativeAllowed = true;
        returnTag = ValueTag.XS_LONG_TAG;
    }

}