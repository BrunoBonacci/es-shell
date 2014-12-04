(ns es-shell.els
  (:require [es-shell.util :refer :all]))


(defn nodes-full [host]
  (GET (str host "_nodes") ))


(defn nodes [host]
  (->>
   (nodes-full host)
   :body
   :nodes
   (map (fn [[id {:keys [name host] :as info}]]
          {:id (clojure.core/name id)
           :name name
           :host host
           :box_type (-> info :attributes :box_type)
           :ip (-> info :network :primary_interface :address)}))))
