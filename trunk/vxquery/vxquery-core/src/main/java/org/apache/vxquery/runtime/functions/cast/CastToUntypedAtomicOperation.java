package org.apache.vxquery.runtime.functions.cast;

import org.apache.vxquery.datamodel.values.ValueTag;

public class CastToUntypedAtomicOperation extends CastToStringOperation {

    public CastToUntypedAtomicOperation() {
        returnTag = ValueTag.XS_UNTYPED_ATOMIC_TAG;
    }

}