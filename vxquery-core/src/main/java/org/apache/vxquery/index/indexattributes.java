package org.apache.vxquery.index;

import java.util.Vector;

import org.xml.sax.Attributes;

public class indexattributes implements Attributes {
    int length;

    Vector<String> names;
    Vector<String> values;
    Vector<String> uris;
    Vector<String> localnames;
    Vector<String> types;
    Vector<String> qnames;

    public indexattributes(Vector<String> n, Vector<String> v, Vector<String> u, Vector<String> l, Vector<String> t,
            Vector<String> q) {
        length = n.size();
        names = n;
        values = v;
        uris = u;
        localnames = l;
        types = t;
        qnames = q;
    }

    /**
     * Return the number of attributes in the list.
     * <p>
     * Once you know the number of attributes, you can iterate through the list.
     * </p>
     * 
     * @return The number of attributes in the list.
     * @see #getURI(int)
     * @see #getLocalName(int)
     * @see #getQName(int)
     * @see #getType(int)
     * @see #getValue(int)
     */
    public int getLength() {
        return length;
    }

    /**
     * Look up an attribute's Namespace URI by index.
     * 
     * @param index
     *            The attribute index (zero-based).
     * @return The Namespace URI, or the empty string if none
     *         is available, or null if the index is out of
     *         range.
     * @see #getLength
     */
    public String getURI(int index) {
        return uris.elementAt(index).toString();
    }

    /**
     * Look up an attribute's local name by index.
     * 
     * @param index
     *            The attribute index (zero-based).
     * @return The local name, or the empty string if Namespace
     *         processing is not being performed, or null
     *         if the index is out of range.
     * @see #getLength
     */
    public String getLocalName(int index) {
        return localnames.elementAt(index).toString();
    }

    /**
     * Look up an attribute's XML qualified (prefixed) name by index.
     * 
     * @param index
     *            The attribute index (zero-based).
     * @return The XML qualified name, or the empty string
     *         if none is available, or null if the index
     *         is out of range.
     * @see #getLength
     */
    public String getQName(int index) {
        return qnames.elementAt(index).toString();
    }

    /**
     * Look up an attribute's type by index.
     * <p>
     * The attribute type is one of the strings "CDATA", "ID", "IDREF", "IDREFS", "NMTOKEN", "NMTOKENS", "ENTITY", "ENTITIES", or "NOTATION" (always in upper case).
     * </p>
     * <p>
     * If the parser has not read a declaration for the attribute, or if the parser does not report attribute types, then it must return the value "CDATA" as stated in the XML 1.0 Recommendation (clause 3.3.3, "Attribute-Value Normalization").
     * </p>
     * <p>
     * For an enumerated attribute that is not a notation, the parser will report the type as "NMTOKEN".
     * </p>
     * 
     * @param index
     *            The attribute index (zero-based).
     * @return The attribute's type as a string, or null if the
     *         index is out of range.
     * @see #getLength
     */
    public String getType(int index) {
        return types.elementAt(index).toString();
    }

    /**
     * Look up an attribute's value by index.
     * <p>
     * If the attribute value is a list of tokens (IDREFS, ENTITIES, or NMTOKENS), the tokens will be concatenated into a single string with each token separated by a single space.
     * </p>
     * 
     * @param index
     *            The attribute index (zero-based).
     * @return The attribute's value as a string, or null if the
     *         index is out of range.
     * @see #getLength
     */
    public String getValue(int index) {
        return values.elementAt(index).toString();
    }

    ////////////////////////////////////////////////////////////////////
    // Name-based query.
    ////////////////////////////////////////////////////////////////////

    /**
     * Look up the index of an attribute by Namespace name.
     * 
     * @param uri
     *            The Namespace URI, or the empty string if
     *            the name has no Namespace URI.
     * @param localName
     *            The attribute's local name.
     * @return The index of the attribute, or -1 if it does not
     *         appear in the list.
     */
    public int getIndex(String uri, String localName) {
        for (int i = 0; i < length; i++) {
            if (localnames.elementAt(i).toString() == localName && uris.elementAt(i).toString() == uri) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Look up the index of an attribute by XML qualified (prefixed) name.
     * 
     * @param qName
     *            The qualified (prefixed) name.
     * @return The index of the attribute, or -1 if it does not
     *         appear in the list.
     */
    public int getIndex(String qName) {
        for (int i = 0; i < length; i++) {
            if (qnames.elementAt(i).toString() == qName) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Look up an attribute's type by Namespace name.
     * <p>
     * See {@link #getType(int) getType(int)} for a description of the possible types.
     * </p>
     * 
     * @param uri
     *            The Namespace URI, or the empty String if the
     *            name has no Namespace URI.
     * @param localName
     *            The local name of the attribute.
     * @return The attribute type as a string, or null if the
     *         attribute is not in the list or if Namespace
     *         processing is not being performed.
     */
    public String getType(String uri, String localName) {
        for (int i = 0; i < length; i++) {
            if (localnames.elementAt(i).toString() == localName && uris.elementAt(i).toString() == uri) {
                return types.elementAt(i).toString();
            }
        }
        return null;
    }

    /**
     * Look up an attribute's type by XML qualified (prefixed) name.
     * <p>
     * See {@link #getType(int) getType(int)} for a description of the possible types.
     * </p>
     * 
     * @param qName
     *            The XML qualified name.
     * @return The attribute type as a string, or null if the
     *         attribute is not in the list or if qualified names
     *         are not available.
     */
    public String getType(String qName) {
        for (int i = 0; i < length; i++) {
            if (qnames.elementAt(i).toString() == qName) {
                return types.elementAt(i).toString();
            }
        }
        return null;
    }

    /**
     * Look up an attribute's value by Namespace name.
     * <p>
     * See {@link #getValue(int) getValue(int)} for a description of the possible values.
     * </p>
     * 
     * @param uri
     *            The Namespace URI, or the empty String if the
     *            name has no Namespace URI.
     * @param localName
     *            The local name of the attribute.
     * @return The attribute value as a string, or null if the
     *         attribute is not in the list.
     */
    public String getValue(String uri, String localName) {
        for (int i = 0; i < length; i++) {
            if (localnames.elementAt(i).toString() == localName && uris.elementAt(i).toString() == uri) {
                return values.elementAt(i).toString();
            }
        }
        return null;
    }

    /**
     * Look up an attribute's value by XML qualified (prefixed) name.
     * <p>
     * See {@link #getValue(int) getValue(int)} for a description of the possible values.
     * </p>
     * 
     * @param qName
     *            The XML qualified name.
     * @return The attribute value as a string, or null if the
     *         attribute is not in the list or if qualified names
     *         are not available.
     */
    public String getValue(String qName) {
        for (int i = 0; i < length; i++) {
            if (qnames.elementAt(i).toString() == qName) {
                return values.elementAt(i).toString();
            }
        }
        return null;
    }

}
