(ns es-shell.cluster
  (:require [es-shell.els :refer :all])
  (:require [es-shell.util :refer :all]))




(defn balance-shards-by-free-disk-space
  "it looks at the free disk space around the cluster,
  and it finds the node with the least free disk space
  available and the node with the most disk space available
  and on these two nodes it finds respectively the biggest
  and the smallest shard. By swapping them places you'll
  get a cluster which is more balanced in terms of free
  disk space."
  [host & {:keys [box-filter-cond]
           :or   {box-filter-cond identity}}]
  (let [cluster (->> host
                     nodes
                     (filter #(= :data (:role %)))
                     (filter box-filter-cond)
                     (sort-by :diskAvail))

        ;; find the two nodes which need to be balanced
        heavy (first cluster) ;; node with least free space
        light (last  cluster) ;; node with most free space

        ;; find the two shards which need to be swapped
        sh-alloc  (shards-allocation host)
        big-shard (->> sh-alloc
                       (filter #(= (:name heavy) (:node %)))
                       (sort-by :shard_size)
                       last)
        small-shard (->> sh-alloc
                         (filter #(= (:name light) (:node %)))
                         (sort-by :shard_size)
                         first)]
    [small-shard big-shard]))



(comment
  (def host "http://localhost:9200/")

  (balance-shards-by-free-disk-space host)

  (def swap (balance-shards-by-free-disk-space host :box-filter-cond #(= "archive" (:box_type %))))

  (apply swap-shards host swap)

  (show (shards-allocation host))

  )
