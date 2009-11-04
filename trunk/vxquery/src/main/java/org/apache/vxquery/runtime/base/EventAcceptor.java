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
package org.apache.vxquery.runtime.base;

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.exceptions.SystemException;

public interface EventAcceptor {
    public void open() throws SystemException;

    public void startDocument() throws SystemException;

    public void endDocument() throws SystemException;

    public void startElement(NameCache nameCache, int nameCode) throws SystemException;

    public void endElement() throws SystemException;

    public void namespace(NameCache nameCache, int prefixCode, int uriCode) throws SystemException;

    public void attribute(NameCache nameCache, int nameCode, CharSequence stringValue) throws SystemException;

    public void text(CharSequence stringValue) throws SystemException;

    public void comment(CharSequence content) throws SystemException;

    public void pi(NameCache nameCache, int nameCode, CharSequence content) throws SystemException;

    public void item(XDMItem item) throws SystemException;

    public void close();
}