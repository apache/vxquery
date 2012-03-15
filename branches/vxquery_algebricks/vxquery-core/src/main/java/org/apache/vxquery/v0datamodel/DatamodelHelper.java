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
package org.apache.vxquery.v0datamodel;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.base.CloseableIterator;

public class DatamodelHelper {
    public static CharSequence serialize(CloseableIterator iter) throws SystemException {
        try {
            XDMValue v;
            StringBuilder sb;
            v = (XDMValue) iter.next();
            if (v == null) {
                return "";
            } else if (v.getDMOKind() == DMOKind.SEQUENCE) {
                sb = new StringBuilder();
            } else {
                XDMItem vi = (XDMItem) v;
                XDMValue v2 = (XDMValue) iter.next();
                if (v2 == null) {
                    return vi.getStringValue();
                }
                sb = new StringBuilder();
                sb.append(vi.getStringValue());
                sb.append(" ");
                v = v2;
            }
            boolean first = true;
            do {
                if (!first) {
                    sb.append(" ");
                }
                first = false;
                switch (v.getDMOKind()) {
                    case SEQUENCE:
                        serialize(sb, (XDMSequence) v);
                        break;

                    default:
                        sb.append(((XDMItem) v).getStringValue());
                        break;
                }
                v = (XDMValue) iter.next();
            } while (v != null);
            return sb;
        } finally {
            iter.close();
        }
    }

    public static void serialize(StringBuilder sb, XDMSequence v) throws SystemException {
        serializeItemIterator(sb, v.createItemIterator());
    }

    public static void serializeItemIterator(StringBuilder sb, CloseableIterator iter) throws SystemException {
        try {
            XDMItem v;
            boolean first = true;
            while ((v = (XDMItem) iter.next()) != null) {
                if (!first) {
                    sb.append(" ");
                }
                first = false;
                sb.append(" ");
                sb.append(v.getStringValue());
            }
        } finally {
            iter.close();
        }
    }
}