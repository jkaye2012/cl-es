(asdf:defsystem #:cl-es
  :serial t
  :depends-on ("cl-redis" "cl-json")
  :components ((:file "package")
               (:file "events")))
