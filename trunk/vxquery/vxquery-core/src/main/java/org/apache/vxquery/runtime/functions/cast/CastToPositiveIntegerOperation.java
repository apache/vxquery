package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToPositiveIntegerOperation extends CastToIntegerOperation {
    boolean negativeAllowed = false;
    int returnTag = ValueTag.XS_POSITIVE_INTEGER_TAG;
}