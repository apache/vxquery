package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUnsignedIntOperation extends CastToLongOperation {
    boolean negativeAllowed = false;
    int returnTag = ValueTag.XS_UNSIGNED_INT_TAG;
}