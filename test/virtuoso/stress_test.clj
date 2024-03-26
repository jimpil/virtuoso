(ns virtuoso.stress-test
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [virtuoso.core :as vt])
  (:import (java.sql Connection)
           (java.util.concurrent CancellationException Executors ScheduledExecutorService TimeUnit)))

(set-agent-send-off-executor! (Executors/newVirtualThreadPerTaskExecutor))
(alter-var-root #'jdbc/get-datasource (fn [_] (constantly :mock/datasource)))
(alter-var-root #'jdbc/get-connection (fn [_] (constantly (reify Connection
                                                            (isClosed [_] false)
                                                            (isValid [_ _] true)
                                                            (unwrap [_ _] :new)
                                                            (close [_] nil)))))
(def logs (agent ""))
(def duration-minutes 5)
(def concurrency 100)
(def ^ScheduledExecutorService scheduled-exec (Executors/newSingleThreadScheduledExecutor))
(defn schedule-stop! [^Runnable stop-fn]
  (.schedule scheduled-exec stop-fn ^long duration-minutes TimeUnit/MINUTES))

(defn start! []
  (let [ds (vt/datasource nil {:maximum-pool-size 5
                               :max-lifetime 60000
                               :idle-timeout 2000
                               :log-fn (fn [& args] (send-off logs (fn [s] (apply println args) s)) nil)})
        futures (->> #(future
                        (while (not (Thread/interrupted))
                          (let [^long ms (inc (rand-int 2000))]
                            (with-open [^Connection conn (.getConnection ds)]
                              (Thread/sleep ms)
                              (send-off logs (fn [s] (doto s (println "done - slept for" ms \- (java.time.Instant/now)))))
                              (send-off logs println "--releasing")))
                          (Thread/sleep ^long (rand-int 1000))))
                     (repeatedly concurrency)
                     doall)]
    (schedule-stop! (partial run! future-cancel futures))
    (try (run! deref futures) ;; block until all futures have been interrupted
         (catch CancellationException _ :done))
    )
  )


