package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUnsignedByteOperation extends CastToShortOperation {
    boolean negativeAllowed = false;
    int returnTag = ValueTag.XS_UNSIGNED_BYTE_TAG;
}