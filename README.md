# Alphatier

Alphatier is a resource management library. It is designed to allow different schedulers to
share the resources of a pool of executors in order to execute tasks with those.

The detailed documentation is located at

* [http://alphatier.io](http://alphatier.io)

An offline version is available by opening the `index.html` file with your browser.

## Development

The source code is managed via [git](http://www.git-scm.com/) and hosted on
[Github](https://github.com/sarnowski/alphatier). See the
[Github help pages](https://help.github.com/articles/working-with-repositories) for more
information.

The build process of this library is managed by [Leiningen](http://leiningen.org/). The
following commands have to be executed at the root of your project checkout:

### Cleanup your project

    lein clean

The command deletes all generated files in order to guarantee a clean and defined project
state.

### Running the test suite

    lein test

The test suite is implemented in the `test/` directory. The test command runs all implemented
unit tests.

### Building a jar artifact

    lein jar

The produced jar file can be used in other JVM based projects. It requires all other dependant
jars on the classpath (see project.clj).

### Generating the documentation

    lein doc

This generates a new documentation `index.html` at the root of your project directory. It uses
comments from the source code.

## License

Copyright (c) 2014 Tobias Sarnowski <tobias@sarnowski.io>

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.