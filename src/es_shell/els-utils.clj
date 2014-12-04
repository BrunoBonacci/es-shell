(ns es-shell.els-utils
  (:require [es-shell.util :refer [rename-key]]))

(defn ->% [tot par]
  (str (int (* (/ par tot) 100)) "%"))


(defn ->status [{status :status :as data}]
  (if status
    (update-in data [:status] keyword)
    data))

(defn ->shard [idx-name replicas shard]
  (let [[num shrd] shard]
    (-> shrd
        ->status
        (assoc :shards% (->% (inc replicas) (:active_shards shrd)))
        (assoc :idx (name idx-name))
        (assoc :shard num)
        (rename-key :active_shards :active)
        (rename-key :relocating_shards :relocating)
        (rename-key :initializing_shards :init)
        (rename-key :unassigned_shards :unassigned))))


(defn ->index [[idx-name {replicas :number_of_replicas :as idx}]]
  (->> idx
       identity
       :shards
       (map #(->shard idx-name replicas %))))
