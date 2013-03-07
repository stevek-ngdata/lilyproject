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
