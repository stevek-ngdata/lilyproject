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
package org.lilyproject.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleUtil {
    private ConsoleUtil() {
    }

    public static boolean promptYesNo(String message, boolean defaultInput) {
        String input = "";
        while (!input.equals("yes") && !input.equals("no") && !input.equals("y") && !input.equals("n")) {
            input = prompt(message, defaultInput ? "yes" : "no");
            input = input.toLowerCase();
        }
        return (input.equals("yes") || input.equals("y"));
    }

    public static String prompt(String message, String defaultInput) {
        System.out.println(message);
        System.out.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String input;
        try {
            input = in.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error reading input from console.", e);
        }
        if (input == null || input.trim().equals("")) {
            input = defaultInput;
        }
        return input;
    }
}
