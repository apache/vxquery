package org.apache.vxquery.common;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.vxquery.functions.BuiltinFunctions;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class VXQueryCommons {

    private final Set<FunctionIdentifier> collectionFunctions = new HashSet<>();

    private final Set<FunctionIdentifier> indexingFunctions = new HashSet<>();
    private static VXQueryCommons vxQueryCommons;

    private VXQueryCommons(){
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_1.getFunctionIdentifier());
        collectionFunctions.add(BuiltinFunctions.FN_COLLECTION_WITH_TAG_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_BUILD_INDEX_ON_COLLECTION_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_COLLECTION_FROM_INDEX_2.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_DELETE_INDEX_1.getFunctionIdentifier());
        indexingFunctions.add(BuiltinFunctions.FN_UPDATE_INDEX_1.getFunctionIdentifier());
    }

    public static VXQueryCommons getInstance() {
        if (vxQueryCommons != null) {
            return vxQueryCommons;
        } else {
            vxQueryCommons = new VXQueryCommons();
            return vxQueryCommons;
        }
    }

    public Set<FunctionIdentifier> getCollectionFunctions() {
        return collectionFunctions;
    }

    public Set<FunctionIdentifier> getIndexingFunctions() {
        return indexingFunctions;
    }


}
