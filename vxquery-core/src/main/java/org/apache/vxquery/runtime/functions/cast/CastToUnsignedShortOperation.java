package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUnsignedShortOperation extends CastToIntOperation {

    public CastToUnsignedShortOperation() {
        negativeAllowed = false;
        returnTag = ValueTag.XS_UNSIGNED_SHORT_TAG;
    }

}