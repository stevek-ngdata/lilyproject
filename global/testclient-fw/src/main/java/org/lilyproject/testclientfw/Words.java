/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.testclientfw;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class Words {
    public enum WordList {
        SMALL_LIST("wordlist-10.txt", 5000), BIG_LIST("wordlist-50.txt", 100000);

        private String file;

        private int estimatedSize;

        WordList(String file, int estimatedSize) {
            this.file = file;
            this.estimatedSize = estimatedSize;
        }
    }

    private static Map<WordList, List<String>> WORDS = new EnumMap<WordList, List<String>>(WordList.class);

    static {
        try {
            for (WordList list : WordList.values()) {
                List<String> result = new ArrayList<String>(list.estimatedSize);
                InputStream is = Words.class.getResourceAsStream(list.file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String word;
                while ((word = reader.readLine()) != null) {
                    if (word.length() > 0) {
                        // The lowerCase is a simple trick to avoid that words like OR, AND, NOT cause
                        // the Solr query parser to fail.
                        result.add(word.toLowerCase());
                    }
                }
                is.close();
                WORDS.put(list, result);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static String get(WordList list) {
        List<String> words = WORDS.get(list);
        return words.get((int)(Math.floor(Math.random() * words.size())));
    }

    public static String get() {
        List<String> words = WORDS.get(WordList.BIG_LIST);
        return words.get((int)(Math.floor(Math.random() * words.size())));
    }

    /**
     * Returns a space-separated string containing the specified amount of words.
     */
    public static String get(WordList list, int amount) {
        StringBuilder buffer = new StringBuilder(20 * amount);
        for (int i = 0; i < amount; i++) {
            if (i > 0) {
                buffer.append(' ');
            }
            buffer.append(get(list));
        }
        return buffer.toString();
    }
}
