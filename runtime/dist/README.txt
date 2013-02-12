This directory contains the things for building a binary Kauri distribution.


How to use
----------

The process:

 1. It is recommended to start from a clean local SVN checkout or export to
    avoid that uncommitted code or other files lingering around will be
    included.

 2. The complete Kauri project should be build: in the source tree root
    execute:
     mvn install

 3. Then the dist can be build: in the 'dist' subdirectory execute:
      mvn assembly:assembly



Background info
---------------
Rather than making use of the parent-child relationships of the projects
and the related moduleSet construct in the assembly configuration, we opted
for defining a separate project to combine all the artifacts we want to ship.

This makes we can make use of the 'repository' construct for including
a Maven-style repository in the binary dist.

This technique of using a separate project is also recommended by Maven
peeps in their book
  Maven: The Definitive Guide
  Tim O'Brien, John Casey, Brian Fox, Bruce Snyder, Jason Van Zyl
  available at
  http://www.sonatype.com/book/reference/assemblies.html
  section "Distribution (Aggregating) Assemblies"