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
package org.lilyproject.tools.tester;

/**
 * Creates strings which can be used as prefix for user defined record ids. It loops round robin through all possible
 * strings (of hex numbers) with the given number of characters.
 *
 * @author Jan Van Besien
 */
public class RoundRobinPrefixGenerator {

    private final int nbrOfChars;

    private int value;

    public RoundRobinPrefixGenerator(int nbrOfChars) {
        if (nbrOfChars < 0) {
            throw new IllegalArgumentException("nbrOfChars should be positive");
        }

        this.nbrOfChars = nbrOfChars;
        this.value = 0;
    }

    public synchronized String next() {
        value++;

        String result = String.format("%0" + nbrOfChars + "x", value);
        if (result.length() > nbrOfChars) {
            value = 0;
            result = String.format("%0" + nbrOfChars + "x", value);
        }

        return result;
    }

    // test
    public static void main(String[] args) throws InterruptedException {
        final RoundRobinPrefixGenerator roundRobinPrefixGenerator = new RoundRobinPrefixGenerator(1);
        while (true) {
            System.out.println(roundRobinPrefixGenerator.next());
            Thread.sleep(100);
        }
    }
}
