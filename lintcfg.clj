(disable-warning
 {:linter :constant-test
  :for-macro 'clojure.core/when
  :if-inside-macroexpansion-of #{'clojure.core/while}
  :within-depth 8
  :reason "allow (while true) forms"})
