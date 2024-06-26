(ns virtuoso.stress-test
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [virtuoso.core :as vt]
            [criterium.core :as criterium]
            [hikari-cp.core])
  (:import (java.io Closeable)
           (java.sql Connection)
           (java.util.concurrent CancellationException Executors ScheduledExecutorService TimeUnit)
           (javax.sql DataSource)))

(set-agent-send-off-executor! (Executors/newVirtualThreadPerTaskExecutor))
(alter-var-root #'jdbc/get-datasource (fn [_] (constantly :mock/datasource)))
(def logs (agent ""))
(alter-var-root #'jdbc/get-connection (fn [_]
                                        (fn [_ _]
                                          (reify Connection
                                            (isClosed [_] false)
                                            (isValid [_ _] (> (rand) 0.5))
                                            (unwrap [_ _] :new)
                                            (close [_]
                                              (send-off logs println "TERMINATING!"))))))

(def duration-minutes 5)
(def concurrency 100)
(def ^ScheduledExecutorService scheduled-exec
  (Executors/newSingleThreadScheduledExecutor))
(defn schedule-stop! [^Runnable stop-fn]
  (.schedule scheduled-exec stop-fn ^long duration-minutes TimeUnit/MINUTES))

(defn start! []
  (let [ds (vt/make-datasource nil {:maximum-pool-size 5
                               :max-lifetime           60000
                               :idle-timeout           1000
                               :log-fn                 (fn [& args] (send-off logs (fn [s] (apply println args) s)) nil)})
        futures (->> #(future
                        (while (not (Thread/interrupted))
                          (let [^long ms (inc (rand-int 2000))]
                            (with-open [^Connection conn (.getConnection ds)]
                              (Thread/sleep ms)
                              (send-off logs (fn [s] (doto s (println "done - slept for" ms \- (java.time.Instant/now)))))
                              (send-off logs println "--releasing")))
                          (Thread/sleep 1000)))
                     (repeatedly concurrency)
                     doall)]
    (schedule-stop! (partial run! future-cancel futures))
    (try (run! deref futures) ;; block until all futures have been interrupted
         (catch CancellationException _
           (.close ^Closeable ds)))))

(defn quick-bench! []
  (with-open [^DataSource ds (vt/make-datasource
                               nil {:pool-size 10
                                    :connection-timeout -1
                                    ;:max-lifetime 5000
                                    ;:idle-timeout 2000
                                    ;:log-fn (fn [& args] (send-off logs (fn [s] (apply println args) s)) nil)
                                    })]
    #_(->> (range 32)
         (pmap (fn))

         )


    (criterium/quick-bench ;; Execution time mean : 7.980545 ns
      (with-open [conn (.getConnection ds)]
        conn
        ;(.unwrap conn nil)
         ;(println (.unwrap conn nil))
         ))))
