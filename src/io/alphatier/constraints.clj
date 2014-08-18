;; ## Constraints
;;
;; In order to prevent total chaos with multiple schedulers, one can define constraints to limit the possibilities of
;; schedulers running amok. Some constraints are already built in like forbidding resource overbooking. Others can be
;; defined by yourself.
(ns io.alphatier.constraints)


;; ### not yet implemented
;;
;; This functionality is not yet implemented.

;; ### Java usage
;;
;; The here defined functions can be accessed via the `Constraints` utility class.
(gen-class
  :name "io.alphatier.Constraints"
  :main false
  :prefix "java-")
