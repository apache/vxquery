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
package org.apache.vxquery.xmlquery.query;

public final class XQueryConstants {
    public static final String XML_PREFIX = "xml";
    public static final String XML_NSURI = "http://www.w3.org/XML/1998/namespace";

    public static final String XS_PREFIX = "xs";
    public static final String XS_NSURI = "http://www.w3.org/2001/XMLSchema";

    public static final String XSEXT_PREFIX = "xsext";
    public static final String XSEXT_NSURI = "http://www.w3.org/2001/XMLSchema-extensions";

    public static final String XSI_PREFIX = "xsi";
    public static final String XSI_NSURI = "http://www.w3.org/2001/XMLSchema-instance";

    public static final String FN_PREFIX = "fn";
    public static final String FN_NSURI = "http://www.w3.org/2005/xpath-functions";

    public static final String LOCAL_PREFIX = "local";
    public static final String LOCAL_NSURI = "http://www.w3.org/2005/xquery-local-functions";

    public static final String ERR_NSURI = "http://www.w3.org/2005/xqt-errors";
    
    public static final String OP_PREFIX = "op";
    public static final String OP_NSURI = "urn:org.apache.vxquery.operators";

    public static final String OPEXT_PREFIX = "opext";
    public static final String OPEXT_NSURI = "urn:org.apache.vxquery.operators-ext";

    private XQueryConstants() {
    }

    public enum OrderDirection {
        ASCENDING, DESCENDING,
    }

    public enum ValidationMode {
        LAX, STRICT
    }

    public enum TypeQuantifier {
        QUANT_QUESTION, QUANT_STAR, QUANT_PLUS
    }
    
    public enum PathType {
        SLASH,
        SLASH_SLASH,
    }
}