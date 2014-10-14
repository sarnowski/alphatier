(ns leiningen.update-desc
  (:require [clojure.string :as string]
            (leiningen [change :as change])))

(defn- escape-double-quotes [s]
  (string/escape s {\" "\\\""}))

(defn- replace-version [version desc]
  (string/replace desc #"<version>.+</version>" (str "<version>" version "</version>")))

(defn update-desc [project]
  (change/change project "description" (comp escape-double-quotes
                                             (partial replace-version (:version project)))))
