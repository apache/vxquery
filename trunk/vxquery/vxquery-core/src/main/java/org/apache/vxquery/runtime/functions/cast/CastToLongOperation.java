package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToLongOperation extends CastToIntegerOperation {
    boolean negativeAllowed = true;
    int returnTag = ValueTag.XS_LONG_TAG;
}