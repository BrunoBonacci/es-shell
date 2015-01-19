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

      curl -s  -XGET 'http://localhost:9200/_cat/nodes?v&h=id,ip,host,master,name,version,diskAvail,heapMax'
      curl -sL -XGET 'http://localhost:9200/_nodes?pretty'
      or
      curl -sL -XGET 'http://localhost:9200/_cat/nodes'

  "
  [host]
  (let [nodes-cat (->> (GET (str host "_cat/nodes?bytes=b&time=h&h=id,ip,host,master,name,version,diskAvail,heapMax,ramMax,uptime,role"))
                       :body
                       (map (fn [doc]
                              (-> doc
                                  (update-in [:uptime]  read-string)
                                  (update-in [:heapMax] str->long)
                                  (update-in [:diskAvail] str->long)
                                  (update-in [:ramMax] str->long)
                                  (update-in [:master] (fn [m] (if (= "*" m) :master :slave)))
                                  (update-in [:role] (fn [r] (case r, "d" :data, "c" :client)))))))
        nodes-cat-map (into {} (map (juxt :name identity) nodes-cat))
        nodes-list  (->>
                     (nodes-full host)
                     :body
                     :nodes
                     (map (fn [[id {:keys [name host] :as info}]]
                            {:id (clojure.core/name id)
                             :name name
                             :host host
                             :box_type (-> info :attributes :box_type)
                             :ip (-> info :network :primary_interface :address)})))
        enrich (fn [{:keys [name] :as nodex}]
                 (when-let [{:keys [heapMax ramMax diskAvail master role]} (get nodes-cat-map name)]
                   (-> nodex
                       (assoc :heapMax   heapMax
                              :ramMax    ramMax
                              :diskAvail diskAvail
                              :master    master
                              :role      role))))]
    (->> nodes-list
         (map enrich)
         (meta-show [:ip :host :name :master :box_type :diskAvail :heapMax :ramMax :role]))))


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

      curl -s  -XGET 'http://localhost:9200/_cat/shards?bytes=b&h=i,s,p,st,d,sto,ip,n'
      curl -sL -XGET 'http://localhost:9200/_cluster/state/_all?pretty'

  "
  [host]
  (let [nodes-map  (index-list-by :id (nodes host))
        shards     (fn [{node :node :as d}]
                     (let [{:keys [name host box_type ip]} (nodes-map node)]
                       (-> d
                           (assoc :node name :host host :ip ip :box_type box_type)
                           (update-in [:relocating_node] (comp :name nodes-map)))))
        index      (fn [idx] (->> idx :shards (mapcat second) (map shards)))
        ;; grab the output of /_cat/shards
        cat-shards (->> (GET (str host "_cat/shards?bytes=b&h=i,s,p,st,d,sto,ip,n"))
                        :body
                        (map (fn [{:keys [i s n p d sto] :as doc}] (-> doc
                                                                      (update-in [:s] str->int)
                                                                      (update-in [:sto] str->long)
                                                                      (update-in [:d] str->long))))
                        (map (fn [{:keys [i s n p d sto] :as doc}] [(str i "/" s "/" (= "p" p) "/" n) doc]))
                        (into {}))
        ;; join the two result-set
        enrich (fn [{:keys [index shard node primary] :as shd}]
                 (when-let [data (get cat-shards (str index "/" shard "/" primary "/" node))]
                   (-> shd
                       (assoc :documents (:d data))
                       (assoc :shard_size (:sto data)))))]
    (->> (GET (str host "_cluster/state/routing_table"))
         :body
         :routing_table
         :indices
         (mapcat (comp index second))
         (map enrich)
         (sort-by (juxt :index :shard (complement :primary)))
         (meta-show [:index :shard :node :box_type :host :ip :state :primary :documents :shard_size :relocating_node]))))





(shards-allocation host)


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

  ;;

  )
