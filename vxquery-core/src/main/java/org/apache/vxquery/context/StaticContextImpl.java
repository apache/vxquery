/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.xml.namespace.QName;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.SchemaType;
import org.apache.vxquery.types.SequenceType;

public class StaticContextImpl implements StaticContext {
    private final StaticContext parent;

    private final Map<String, String> namespaceMap;

    private final Map<QName, XQueryVariable> variableMap;

    protected final Map<String, Collation> collationMap;

    protected final Map<QName, Function[]> functionMap;

    protected final Map<String, SequenceType> documentTypeMap;

    protected final Map<String, SequenceType> collectionTypeMap;

    protected final List<Pair<String, List<String>>> moduleImports;

    protected final List<Pair<String, List<String>>> schemaImports;

    protected final Map<QName, SchemaType> schemaTypeMap;

    protected final Map<SequenceType, Integer> sequenceTypeMap;

    protected final List<SequenceType> sequenceTypeList;

    protected final Map<QName, AttributeType> attributeDeclarationMap;

    protected final Map<QName, ElementType> elementDeclarationMap;

    protected final Map<QName, String> options;

    private BoundarySpaceProperty boundarySpaceProperty;

    private String defaultFunctionNamespaceUri;

    private String defaultElementNamespaceUri;

    private OrderingModeProperty orderingModeProperty;

    private EmptyOrderProperty emptyOrderProperty;

    private String defaultCollation;

    private String baseUri;

    private ConstructionModeProperty constructionModeProperty;

    private CopyNamespacesModeProperty copyNamespacesModeProperty;

    private SequenceType defaultCollectionType;

    private int typeCounter;

    public StaticContextImpl(StaticContext parent) {
        this.parent = parent;
        namespaceMap = new LinkedHashMap<String, String>();
        variableMap = new LinkedHashMap<QName, XQueryVariable>();
        collationMap = new LinkedHashMap<String, Collation>();
        functionMap = new LinkedHashMap<QName, Function[]>();
        documentTypeMap = new LinkedHashMap<String, SequenceType>();
        collectionTypeMap = new LinkedHashMap<String, SequenceType>();
        moduleImports = new ArrayList<Pair<String, List<String>>>();
        schemaImports = new ArrayList<Pair<String, List<String>>>();
        schemaTypeMap = new LinkedHashMap<QName, SchemaType>();
        sequenceTypeMap = new HashMap<SequenceType, Integer>();
        sequenceTypeList = new ArrayList<SequenceType>();
        attributeDeclarationMap = new LinkedHashMap<QName, AttributeType>();
        elementDeclarationMap = new LinkedHashMap<QName, ElementType>();
        options = new LinkedHashMap<QName, String>();
        typeCounter = parent == null ? 0 : parent.getMaxSequenceTypeCode();
    }

    @Override
    public StaticContext getParent() {
        return parent;
    }

    @Override
    public String lookupNamespaceUri(String prefix) {
        if (namespaceMap.containsKey(prefix)) {
            return namespaceMap.get(prefix);
        }
        if (parent != null) {
            return parent.lookupNamespaceUri(prefix);
        }
        return null;
    }

    @Override
    public void registerNamespaceUri(String prefix, String uri) {
        namespaceMap.put(prefix, uri);
    }

    @Override
    public Collation lookupCollation(String collationName) {
        if (collationMap.containsKey(collationName)) {
            return collationMap.get(collationName);
        }
        if (parent != null) {
            return parent.lookupCollation(collationName);
        }
        return null;
    }

    @Override
    public void registerCollation(String collationName, Collation collation) {
        collationMap.put(collationName, collation);
    }

    @Override
    public Function lookupFunction(QName functionName, int arity) {
        if (functionMap.containsKey(functionName)) {
            Function[] fns = functionMap.get(functionName);
            if (fns != null && fns.length > arity && fns[arity] != null) {
                return fns[arity];
            }
        }
        if (parent != null) {
            return parent.lookupFunction(functionName, arity);
        }
        return null;
    }

    @Override
    public Function[] lookupFunctions(QName functionName) {
        if (functionMap.containsKey(functionName)) {
            return functionMap.get(functionName);
        }
        if (parent != null) {
            return parent.lookupFunctions(functionName);
        }
        return null;
    }

    @Override
    public void registerFunction(Function function) {
        Function fns[] = functionMap.get(function.getName());
        int arity = function.getSignature().getArity();
        if (fns == null) {
            fns = new Function[arity + 1];
            fns[arity] = function;
            functionMap.put(function.getName(), fns);
        } else if (fns.length <= arity) {
            Function newFns[] = new Function[arity + 1];
            System.arraycopy(fns, 0, newFns, 0, fns.length);
            newFns[arity] = function;
            functionMap.put(function.getName(), newFns);
        } else {
            fns[arity] = function;
        }
    }

    @Override
    public Iterator<Function> listFunctions() {
        return new Iterator<Function>() {
            private Iterator<Function[]> faIter = functionMap.values().iterator();
            private Function[] fa = null;
            private int faIdx = 0;

            @Override
            public boolean hasNext() {
                fetchNext();
                return fa != null;
            }

            @Override
            public Function next() {
                fetchNext();
                if (fa == null) {
                    throw new NoSuchElementException();
                }
                return fa[faIdx++];
            }

            private void fetchNext() {
                while (true) {
                    if (fa != null && faIdx < fa.length) {
                        if (fa[faIdx] != null) {
                            break;
                        }
                        ++faIdx;
                    } else {
                        if (faIter.hasNext()) {
                            fa = faIter.next();
                            faIdx = 0;
                        } else {
                            fa = null;
                            break;
                        }
                    }
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public SequenceType lookupDocumentType(String docUri) {
        if (documentTypeMap.containsKey(docUri)) {
            return documentTypeMap.get(docUri);
        }
        if (parent != null) {
            return parent.lookupDocumentType(docUri);
        }
        return null;
    }

    @Override
    public void registerDocumentType(String docUri, SequenceType type) {
        documentTypeMap.put(docUri, type);
    }

    @Override
    public XQueryVariable lookupVariable(QName name) {
        if (variableMap.containsKey(name)) {
            return variableMap.get(name);
        }
        if (parent != null) {
            return parent.lookupVariable(name);
        }
        return null;
    }

    @Override
    public void registerVariable(XQueryVariable var) {
        variableMap.put(var.getName(), var);
    }

    @Override
    public Iterator<XQueryVariable> listVariables() {
        return Collections.unmodifiableCollection(variableMap.values()).iterator();
    }

    @Override
    public SequenceType lookupCollectionType(String collectionUri) {
        if (collectionTypeMap.containsKey(collectionUri)) {
            return collectionTypeMap.get(collectionUri);
        }
        if (parent != null) {
            return parent.lookupCollectionType(collectionUri);
        }
        return null;
    }

    @Override
    public void registerCollectionType(String collectionUri, SequenceType type) {
        collectionTypeMap.put(collectionUri, type);
    }

    @Override
    public Iterator<Pair<String, List<String>>> listModules() {
        return new ConcatenatingIterator<Pair<String, List<String>>>() {
            @Override
            protected Iterator<Pair<String, List<String>>> getCurrentIterator() {
                return moduleImports.iterator();
            }

            @Override
            protected Iterator<Pair<String, List<String>>> getParentIterator() {
                if (parent != null) {
                    return parent.listModules();
                }
                return null;
            }
        };
    }

    @Override
    public void registerModuleImport(String uri, List<String> locations) {
        moduleImports.add(Pair.<String, List<String>> of(uri, locations));
    }

    @Override
    public Iterator<Pair<String, List<String>>> listSchemas() {
        return new ConcatenatingIterator<Pair<String, List<String>>>() {
            @Override
            protected Iterator<Pair<String, List<String>>> getCurrentIterator() {
                return schemaImports.iterator();
            }

            @Override
            protected Iterator<Pair<String, List<String>>> getParentIterator() {
                if (parent != null) {
                    return parent.listSchemas();
                }
                return null;
            }
        };
    }

    @Override
    public void registerSchemaImport(String uri, List<String> locations) {
        schemaImports.add(Pair.<String, List<String>> of(uri, locations));
    }

    @Override
    public SchemaType lookupSchemaType(QName name) {
        if (schemaTypeMap.containsKey(name)) {
            return schemaTypeMap.get(name);
        }
        if (parent != null) {
            return parent.lookupSchemaType(name);
        }
        return null;
    }

    @Override
    public void registerSchemaType(QName name, SchemaType type) {
        schemaTypeMap.put(name, type);
    }

    @Override
    public int lookupSequenceType(SequenceType type) {
        if (sequenceTypeMap.containsKey(type)) {
            return sequenceTypeMap.get(type);
        }
        if (parent != null) {
            return parent.lookupSequenceType(type);
        }
        return -1;
    }

    @Override
    public SequenceType lookupSequenceType(int code) {
        int maxParentTypeCode = parent == null ? 0 : parent.getMaxSequenceTypeCode();
        if (code >= maxParentTypeCode) {
            return sequenceTypeList.get(code - maxParentTypeCode);
        }
        return parent.lookupSequenceType(code);
    }

    @Override
    public int encodeSequenceType(SequenceType type) {
        int code = lookupSequenceType(type);
        if (code == -1) {
            code = typeCounter++;
            sequenceTypeMap.put(type, code);
            sequenceTypeList.add(type);
            return code;
        }
        if (sequenceTypeMap.containsKey(type)) {
            return sequenceTypeMap.get(type);
        }
        return -1;
    }

    List<SequenceType> getSequenceTypeList() {
        return sequenceTypeList;
    }

    @Override
    public int getMaxSequenceTypeCode() {
        return typeCounter;
    }

    @Override
    public AttributeType lookupAttributeDeclaration(QName name) {
        if (attributeDeclarationMap.containsKey(name)) {
            return attributeDeclarationMap.get(name);
        }
        if (parent != null) {
            return parent.lookupAttributeDeclaration(name);
        }
        return null;
    }

    @Override
    public void registerAttributeDeclaration(QName name, AttributeType attrDecl) {
        attributeDeclarationMap.put(name, attrDecl);
    }

    @Override
    public ElementType lookupElementDeclaration(QName name) {
        if (elementDeclarationMap.containsKey(name)) {
            return elementDeclarationMap.get(name);
        }
        if (parent != null) {
            return parent.lookupElementDeclaration(name);
        }
        return null;
    }

    @Override
    public void registerElementDeclaration(QName name, ElementType elemDecl) {
        elementDeclarationMap.put(name, elemDecl);
    }

    @Override
    public BoundarySpaceProperty getBoundarySpaceProperty() {
        if (boundarySpaceProperty != null) {
            return boundarySpaceProperty;
        }
        if (parent != null) {
            return parent.getBoundarySpaceProperty();
        }
        return null;
    }

    @Override
    public void setBoundarySpaceProperty(BoundarySpaceProperty boundarySpaceProperty) {
        this.boundarySpaceProperty = boundarySpaceProperty;
    }

    @Override
    public String getDefaultFunctionNamespaceUri() {
        if (defaultFunctionNamespaceUri != null) {
            return defaultFunctionNamespaceUri;
        }
        if (parent != null) {
            return parent.getDefaultFunctionNamespaceUri();
        }
        return null;
    }

    @Override
    public void setDefaultFunctionNamespaceUri(String uri) {
        this.defaultFunctionNamespaceUri = uri;
    }

    @Override
    public String getDefaultElementNamespaceUri() {
        if (defaultElementNamespaceUri != null) {
            return defaultElementNamespaceUri;
        }
        if (parent != null) {
            return parent.getDefaultElementNamespaceUri();
        }
        return null;
    }

    @Override
    public void setDefaultElementNamespaceUri(String uri) {
        this.defaultElementNamespaceUri = uri;
    }

    @Override
    public OrderingModeProperty getOrderingModeProperty() {
        if (orderingModeProperty != null) {
            return orderingModeProperty;
        }
        if (parent != null) {
            return parent.getOrderingModeProperty();
        }
        return null;
    }

    @Override
    public void setOrderingModeProperty(OrderingModeProperty orderingMode) {
        this.orderingModeProperty = orderingMode;
    }

    @Override
    public EmptyOrderProperty getEmptyOrderProperty() {
        if (emptyOrderProperty != null) {
            return emptyOrderProperty;
        }
        if (parent != null) {
            return parent.getEmptyOrderProperty();
        }
        return null;
    }

    @Override
    public void setEmptyOrderProperty(EmptyOrderProperty emptyOrder) {
        this.emptyOrderProperty = emptyOrder;
    }

    @Override
    public String getDefaultCollation() {
        if (defaultCollation != null) {
            return defaultCollation;
        }
        if (parent != null) {
            return parent.getDefaultCollation();
        }
        return null;
    }

    @Override
    public void setDefaultCollation(String defaultCollation) {
        this.defaultCollation = defaultCollation;
    }

    @Override
    public String getBaseUri() {
        if (baseUri != null) {
            return baseUri;
        }
        if (parent != null) {
            return parent.getBaseUri();
        }
        return null;
    }

    @Override
    public void setBaseUri(String baseUri) {
        this.baseUri = baseUri;
    }

    @Override
    public ConstructionModeProperty getConstructionModeProperty() {
        if (constructionModeProperty != null) {
            return constructionModeProperty;
        }
        if (parent != null) {
            return parent.getConstructionModeProperty();
        }
        return null;
    }

    @Override
    public void setConstructionModeProperty(ConstructionModeProperty constructionMode) {
        this.constructionModeProperty = constructionMode;
    }

    @Override
    public CopyNamespacesModeProperty getCopyNamespacesModeProperty() {
        if (copyNamespacesModeProperty != null) {
            return copyNamespacesModeProperty;
        }
        if (parent != null) {
            return parent.getCopyNamespacesModeProperty();
        }
        return null;
    }

    @Override
    public void setCopyNamespacesModeProperty(CopyNamespacesModeProperty copyNamespacesMode) {
        this.copyNamespacesModeProperty = copyNamespacesMode;
    }

    @Override
    public SequenceType getDefaultCollectionType() {
        if (defaultCollectionType != null) {
            return defaultCollectionType;
        }
        if (parent != null) {
            return parent.getDefaultCollectionType();
        }
        return null;
    }

    @Override
    public void setDefaultCollectionType(SequenceType type) {
        this.defaultCollectionType = type;
    }

    @Override
    public void setOption(QName name, String value) {
        options.put(name, value);
    }

    @Override
    public String getOption(QName name) {
        if (options.containsKey(name)) {
            return options.get(name);
        }
        if (parent != null) {
            return parent.getOption(name);
        }
        return null;
    }

    public IStaticContextFactory createFactory() {
        return StaticContextImplFactory.createInstance(this);
    }

    private abstract class ConcatenatingIterator<T> implements Iterator<T> {
        Iterator<T> currListIter = getCurrentIterator();
        Iterator<T> parentIter = null;
        T nextItem = null;

        @Override
        public boolean hasNext() {
            fetchNext();
            return nextItem != null;
        }

        protected abstract Iterator<T> getCurrentIterator();

        protected abstract Iterator<T> getParentIterator();

        @Override
        public T next() {
            if (hasNext()) {
                T item = nextItem;
                nextItem = null;
                return item;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void fetchNext() {
            if (nextItem != null) {
                return;
            }
            if (currListIter != null) {
                if (currListIter.hasNext()) {
                    nextItem = currListIter.next();
                } else {
                    currListIter = null;
                    parentIter = getParentIterator();
                }
            }
            if (nextItem == null && parentIter != null) {
                if (parentIter.hasNext()) {
                    nextItem = parentIter.next();
                } else {
                    parentIter = null;
                }
            }
        }
    }
}