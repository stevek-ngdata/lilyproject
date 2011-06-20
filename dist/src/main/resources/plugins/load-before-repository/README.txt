plugins: load-before-repository
===============================

Here you can drop extensions to be started before the repository itself
starts.

Extensions are in the form of Kauri modules (jar files) or wiring XML
files (that point to extensions stored in a Maven repository).

The only kind of extensions currently supported are Repository Decorators.
See http://docs.outerthought.org/lily-docs-current/512-lily.html