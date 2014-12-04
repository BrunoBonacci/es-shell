(ns es-shell.util
  (:require [clojure.string :as s])
  (:require [cheshire.core :as json])
  (:require [clj-http.client :as http])
  (:require [clojure.pprint :as pp]))


(defn to-json
  "Convert a Clojure data structure into it's json pretty print equivalent
   or compact version.
   usage:

   (to-json {:a \"value\" :b 123})
   ;=> {
   ;=>   \"a\" : \"value\",
   ;=>   \"b\" : 123
   ;=> }

   (to-json {:a \"value\" :b 123} :compact true)
   ;=> {\"a\":\"value\",\"b\":123}
   "
  [data & {:keys [compact] :or {compact false}}]
  (if-not data
    ""
    (-> data
        (json/generate-string {:pretty (not compact)})
        ((fn [s] (if (not compact) (str s \newline) s))))))


(defn from-json
  "Convert a json string into a Clojure data structure
   with keyword as keys"
  [data]
  (if-not data
    nil
    (-> data
        (json/parse-string true))))


(defn GET [url & {:as opts}]
  (http/get url
            (merge
             {:socket-timeout 5000
              :conn-timeout 10000
              :throw-exceptions false
              :accept :json
              :as :json
              :content-type :json
              :coerce :always }
             opts)))


(defn POST [url & {:keys [body] :as opts}]
  (http/post url
            (merge
             {:socket-timeout 5000
              :conn-timeout 10000
              :throw-exceptions false
              :accept :json
              :as :json
              :content-type :json
              :coerce :always }
             (if (map? body)
               (assoc (dissoc opts :body) :form-params body)
               opts))))


(defn PUT [url & {:keys [body] :as opts}]
  (http/put url
            (merge
             {:socket-timeout 5000
              :conn-timeout 10000
              :throw-exceptions false
              :accept :json
              :as :json
              :content-type :json
              :coerce :always }
             (if (map? body)
               (assoc (dissoc opts :body) :form-params body)
               opts))))

(defn DELETE [url & {:keys [body] :as opts}]
  (http/delete url
            (merge
             {:socket-timeout 5000
              :conn-timeout 10000
              :throw-exceptions false
              :accept :json
              :as :json
              :content-type :json
              :coerce :always }
             (if (map? body)
               (assoc (dissoc opts :body) :form-params body)
               opts))))


(defn show
  ([v] (pp/print-table v))
  ([keys v] (pp/print-table keys v)))
