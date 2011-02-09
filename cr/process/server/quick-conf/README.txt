The quick-conf is different from conf in that it is optimized to start Lily more quickly.
This is useful in combination with launch-hadoop, where you always start from scratch.

The quick-conf is also used by the process testcases.

To use it, combine it with the normal conf:

lily-server -c quick-conf:conf