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

package org.apache.vxquery.runtime.functions.index;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;

/**
 * Filters {@link StandardTokenizer} with {@link StandardFilter},
 * and {@link StopFilter}, using a list of
 * English stop words.
 * <a name="version"/>
 * <p>
 * You must specify the required {@link Version}
 * compatibility when creating StandardAnalyzer:
 * <ul>
 * <li>As of 3.4, Hiragana and Han characters are no longer wrongly split
 * from their combining characters. If you use a previous version number,
 * you get the exact broken behavior for backwards compatibility.
 * <li>As of 3.1, StandardTokenizer implements Unicode text segmentation,
 * and StopFilter correctly handles Unicode 4.0 supplementary characters
 * in stopwords. {@link ClassicTokenizer} and {@link ClassicAnalyzer}
 * are the pre-3.1 implementations of StandardTokenizer and
 * StandardAnalyzer.
 * <li>As of 2.9, StopFilter preserves position increments
 * <li>As of 2.4, Tokens incorrectly identified as acronyms
 * are corrected (see <a href="https://issues.apache.org/jira/browse/LUCENE-1068">LUCENE-1068</a>)
 * </ul>
 */
public final class CaseSensitiveAnalyzer extends StopwordAnalyzerBase {

    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

    /**
     * An unmodifiable set containing some common English words that are usually not
     * useful for searching.
     */
    public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

    /**
     * Builds an analyzer with the given stop words.
     * 
     * @param stopWords
     *            stop words
     */
    public CaseSensitiveAnalyzer(CharArraySet stopWords) {
        super(stopWords);
    }

    /**
     * Builds an analyzer with the default stop words ({@link #STOP_WORDS_SET}).
     */
    public CaseSensitiveAnalyzer() {
        this(STOP_WORDS_SET);
    }

    /**
     * Builds an analyzer with the stop words from the given reader.
     * 
     * @see WordlistLoader#getWordSet(Reader)
     * @param stopwords
     *            Reader to read stop words from
     */
    public CaseSensitiveAnalyzer(Reader stopwords) throws IOException {
        this(loadStopwordSet(stopwords));
    }

    /**
     * Set maximum allowed token length. If a token is seen
     * that exceeds this length then it is discarded. This
     * setting only takes effect the next time tokenStream or
     * tokenStream is called.
     */
    public void setMaxTokenLength(int length) {
        maxTokenLength = length;
    }

    /**
     * @see #setMaxTokenLength
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer src;
        if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
            StandardTokenizer t = new StandardTokenizer();
            t.setMaxTokenLength(maxTokenLength);
            src = t;
        } else {
            StandardTokenizer40 t = new StandardTokenizer40();
            t.setMaxTokenLength(maxTokenLength);
            src = t;
        }
        TokenStream tok = new StandardFilter(src);
        tok = new StopFilter(tok, stopwords);
        return new TokenStreamComponents(src, tok) {
            @Override
            protected void setReader(final Reader reader) {
                int m = CaseSensitiveAnalyzer.this.maxTokenLength;
                if (src instanceof StandardTokenizer) {
                    ((StandardTokenizer) src).setMaxTokenLength(m);
                } else {
                    ((StandardTokenizer40) src).setMaxTokenLength(m);
                }
                super.setReader(reader);
            }
        };
    }
}