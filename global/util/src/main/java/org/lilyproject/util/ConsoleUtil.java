package org.lilyproject.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleUtil {
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
        if (input == null || input.trim().equals(""))
            input = defaultInput;
        return input;
    }
}
