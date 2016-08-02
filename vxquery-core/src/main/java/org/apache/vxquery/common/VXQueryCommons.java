package org.apache.vxquery.common;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.vxquery.functions.BuiltinFunctions;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class VXQueryCommons {

    public static final Set<FunctionIdentifier> collectionFunctions = new HashSet<>();

    public static final Set<FunctionIdentifier> indexingFunctions = new HashSet<>();

    static {
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier());
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_WITH_TAG_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_COLLECTION_FROM_INDEX_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier());
    }

}
