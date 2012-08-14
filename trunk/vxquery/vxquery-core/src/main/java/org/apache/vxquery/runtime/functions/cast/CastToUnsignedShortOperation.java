package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUnsignedShortOperation extends CastToIntOperation {
    boolean negativeAllowed = false;
    int returnTag = ValueTag.XS_UNSIGNED_SHORT_TAG;
}