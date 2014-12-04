(ns es-shell.els
  (:require [es-shell.util :refer :all])
  (:require [es-shell.els-utils :refer :all]))


(defn nodes-full
  "returns the node list with the full node information.
   same as:

      curl -sL -XGET 'http://localhost:9200/_nodes?pretty'

  "
  [host]
  (GET (str host "_nodes") ))


(defn nodes
  "returns a synthetic view of cluster's nodes
   short of:

      curl -sL -XGET 'http://localhost:9200/_nodes?pretty'
      or
      curl -sL -XGET 'http://localhost:9200/_cat/nodes'

  "
  [host]
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



(defn health
  "returns the cluster health
   same as:

      curl -sL -XGET 'http://localhost:9200/_cluster/health?pretty'

  "
  [host]
  (-> (GET (str host "_cluster/health"))
      :body
      ->status))


(defn status
  "wheather the cluster is: :green, :yellow or :red"
  [host]
  (:status (health host)))


(defn shards-status
  "returns the cluster's allocation
   same as:

      curl -sL -XGET 'http://localhost:9200/_cluster/health?level=shards&pretty'

  "
  [host]
  (->> (GET (str host "_cluster/health/daily-events-2014-11-26?level=shards"))
       :body
       :indices
       (mapcat ->index)))




(comment

  (def host "http://localhost:9200/")

  (show
   (nodes host))

  (health host)

  (status host)

  (show (shards-status host))


  )
