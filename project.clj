(defproject io.alphatier/alphatier "0.2.0-SNAPSHOT"

  :description "
Alphatier is a resource management library. It is designed to allow different
schedulers to share the resources of a pool of executors in order to execute
tasks with those.

Read the [detailed documentation](#io.alphatier.pools) below to get an in-depth
understanding.

## License

Copyright &copy; 2014 [Tobias Sarnowski](mailto:tobias@sarnowski.io),
[Willi Schönborn](mailto:w.schoenborn@gmail.com)

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED \"AS IS\" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

## Usage

The library is written in [Clojure](http://clojure.org/) and  is available in the
[central Maven repository](https://repo.maven.org/maven2/io/alphatier/alphatier):

    <dependency>
        <groupId>io.alphatier</groupId>
        <artifactId>alphatier</artifactId>
        <version>0.1.0</version>
    </dependency>

The library is written in pure Clojure without [ahead-of-time compilation](http://clojure.org/compilation).
This means, that the library does not contain any *.class files. If you work with
Clojure, this is not a problem but if you like to use the library from another
JVM language (like Java, Scala or Groovy), you can use
[Clojure's built-in tools](http://clojure.org/java_interop#Java%20Interop-Calling%20Clojure%20From%20Java)
for interoperability or try our Java library:

[https://github.com/sarnowski/alphatier-java](https://github.com/sarnowski/alphatier-java)

### Development

If you like to change this library, please have a look at the [README](README.md). Development is done via
[Github](https://github.com/sarnowski/alphatier).
"

  :url "http://alphatier.io"

  :license {:name "ISC License"
            :url "http://opensource.org/licenses/ISC"
            :distribution :repo}

  :scm {:url "git@github.com:sarnowski/alphatier.git"}

  :pom-addition [:developers
                 [:developer
                  [:name "Tobias Sarnowski"]
                  [:url "http://www.sarnowski.io"]
                  [:email "tobias@sarnowski.io"]
                  [:timezone "+1"]]
                 [:developer
                  [:name "Willi Schönborn"]
                  [:url "http://codereligion.com/"]
                  [:email "w.schoenborn@gmail.com"]
                  [:timezone "+1"]]]

  :min-lein-version "2.0.0"

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.incubator "0.1.3"]]

  :plugins [[lein-marginalia "0.7.1"]]
  :aliases {"doc" ["marg"
                   "-n" "Alphatier"
                   "-d" "."
                   "-f" "index.html"
                   "-c" "doc/style.css"
                   "src/io/alphatier/pools.clj"
                   "src/io/alphatier/schedulers.clj"
                   "src/io/alphatier/constraints.clj"
                   "src/io/alphatier/executors.clj"]}

  :signing {:gpg-key "tobias@sarnowski.io"}

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/" :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/" :creds :gpg}})
