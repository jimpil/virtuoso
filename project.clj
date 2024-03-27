(defproject com.github.jimpil/virtuoso "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.12.0-alpha9"]
                 [com.github.seancorfield/next.jdbc "1.3.925"]]
  :repl-options {:init-ns virtuoso.core}
  :profiles
  {:dev
   {:dependencies [[criterium "0.4.6"]
                   [hikari-cp "3.0.1"]]}}
  )
