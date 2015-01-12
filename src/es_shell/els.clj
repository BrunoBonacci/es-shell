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
  (->> (GET (str host "_cluster/health?level=shards"))
       :body
       :indices
       (mapcat ->index)
       (sort-by (juxt :index :shard))
       (meta-show [:index :shard :status :shards% :primary_active :active :init :relocating :unassigned])))



(defn shards-allocation
  "returns the cluster shards allocation
   same as:

      curl -sL -XGET 'http://localhost:9200/_cluster/state/_all?pretty'

  "
  [host]
  (let [nodes-map  (index-list-by :id (nodes host))
        shards     (fn [{node :node :as d}]
                     (let [{:keys [name host box_type ip]} (nodes-map node)]
                       (-> d
                           (assoc :node name :host host :ip ip :box_type box_type)
                           (update-in [:relocating_node] (comp :name nodes-map)))))
        index      (fn [idx] (->> idx :shards (mapcat second) (map shards)))]
    (->> (GET (str host "_cluster/state/routing_table"))
         :body
         :routing_table
         :indices
         (mapcat (comp index second))
         (sort-by (juxt :index :shard (complement :primary)))
         (meta-show [:index :shard :node :box_type :host :ip :state :primary :relocating_node]))))


(comment

  (def host "http://localhost:9200/")

  (show
   (nodes host))


  (->> host
       nodes
       (sort-by :box_type)
       show)

  (nodes-full host)

  (health host)

  (status host)

  (show
   (shards-status host))

  (show
   (shards-allocation host))

  )
