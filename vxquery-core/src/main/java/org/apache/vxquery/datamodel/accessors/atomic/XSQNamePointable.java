package org.apache.vxquery.datamodel.accessors.atomic;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class XSQNamePointable extends AbstractPointable {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSQNamePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    /* TODO Do we need this?
    public void setQName(IPointable uri, IPointable prefix, IPointable localName) {
        int uriLength = uri.getLength();
        int prefixLength = prefix.getLength();
        set(uri);
        set(prefix.getByteArray(), prefixLength, uriLength);
        set(localName.getByteArray(), localName.getLength(), (uriLength + prefixLength));
    }
    */

    public int getUriLength() {
        return UTF8StringPointable.getUTFLength(bytes, start) + 2;
    }

    public static int getUriLength(byte[] bytes, int start) {
        return UTF8StringPointable.getUTFLength(bytes, start) + 2;
    }

    public int getPrefixLength() {
        return UTF8StringPointable.getUTFLength(bytes, start + getUriLength()) + 2;
    }

    public static int getPrefixLength(byte[] bytes, int start) {
        return UTF8StringPointable.getUTFLength(bytes, start + getUriLength(bytes, start)) + 2;
    }

    public int getLocalNameLength() {
        return UTF8StringPointable.getUTFLength(bytes, start + getUriLength() + getPrefixLength()) + 2;
    }

    public static int getLocalNameLength(byte[] bytes, int start) {
        return UTF8StringPointable.getUTFLength(bytes,
                start + getUriLength(bytes, start) + getPrefixLength(bytes, start)) + 2;
    }

    public void getUri(IPointable stringp) {
        stringp.set(bytes, start, getUriLength());
    }

    public static void getUri(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start, getUriLength(bytes, start));
    }

    public void getPrefix(IPointable stringp) {
        stringp.set(bytes, start + getUriLength(), getPrefixLength());
    }

    public static void getPrefix(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start + getUriLength(bytes, start), getPrefixLength(bytes, start));
    }

    public void getLocalName(IPointable stringp) {
        stringp.set(bytes, start + getUriLength() + getPrefixLength(), getLocalNameLength());
    }

    public static void getLocalName(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start + getUriLength(bytes, start) + getPrefixLength(bytes, start),
                getLocalNameLength(bytes, start));
    }
}
