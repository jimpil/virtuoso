(ns virtuoso.core
  (:require [next.jdbc :as jdbc]
            [virtuoso.internal.impl :as internal])
  (:import  [java.io Closeable]
            [java.sql SQLException SQLFeatureNotSupportedException]
            [javax.sql DataSource]
            [java.util.concurrent LinkedTransferQueue TimeUnit]
            [virtuoso.internal ReusableConnection]))

(defn datasource
  "Returns a `java.sql.DataSource` which pools/reuses connections.
   It does this by spawning virtual-threads, all waiting to
   `TransferQueue.tryTransfer` to the first available consumer.
   In other words, there is a 'pool' of virtual-threads which
   are synchronized/coordinated via a `LinkedTransferQueue`.
   The premise is that it is simpler/cheaper to simply block
   virtual-threads, than trying to maintain an auto-scalable/shrinkable
   pool, and the numbers seem to back that up. This DataSource is able
   of giving you back a physical `Connection` in just over 9ns,
   which in terms of ops/ms, is more than double of what HikariCP claims.
   The options stay largely the same with HikariCP, except `:minimum-idle`,
   which simply doesn't make sense. In this design, during periods of
   inactivity, the 'pool' will scale-down to essentially zero w/o affecting
   readiness - i.e. all threads will be blocked, waiting to server someone!
   Similarly, the whole 'keep-alive' mechanism is approached differently.
   Instead of separately scheduled validity checks (and the complications
   this entails), like HikariCP does, this happens every time a transfer fails
   - never while waiting (i.e. in the pool). This may(?) become a reason for
   shorter `:max-lifetime` on some systems/drivers. Finally, `connection-timeout`
   does mean the same thing as in HikariCP, but can be zero or negative (to disable),
   and if exceeded will not throw, but rather, will create a new (non-reusable)
   connection from the <db-spec> (to accommodate spikes). A 2-arg (message, data)
   nil-returning/non-blocking <log-fn> can optionally be provided. It will be called
   whenever a reusable connection is either closed/(re)used, or a non-reusable
   connection is created (i.e. <connection-timeout> elapsed)."
  ^DataSource
  [db-spec {:keys [maximum-pool-size ^long connection-timeout log-fn]
            :or {connection-timeout 30000
                 log-fn internal/noop}
            :as opts}]
  (let [ds      (jdbc/get-datasource db-spec)
        ltq     (LinkedTransferQueue.)
        closed? (volatile! false)
        threads (mapv
                  (fn [i] (internal/thread-conn ds i closed? ltq opts))
                  (range maximum-pool-size))
        take! (if (pos? connection-timeout)
                #(.poll ltq connection-timeout TimeUnit/MILLISECONDS)
                #(.take ltq))]
    (reify
      DataSource
      (getConnection [_]
        (loop [i 0
               [^ReusableConnection conn
                thread-index] (take!)]
          (cond
            (nil? conn)
            (if @closed?
              (throw
                (SQLException. "Datasource has been closed!"))
              (do  ;; connection-timeout
                (log-fn "Creating non-reusable connection (slow path)!" {})
                (jdbc/get-connection db-spec opts)))

            (.isClosed conn) ;; better safe than sorry
            (do ;; somehow the underlying connection was closed (OS/driver?)
              (log-fn "Got a closed connection - retrying from different producer!"
                      {:conn conn
                       :retry i})
              (internal/interrupt-thread! (threads thread-index)) ;; trigger replenish
              (recur (unchecked-inc i) (take!)))
            ;; happy path (i.e. reusing connection)
            :else conn)))
      (getConnection [_ _ _]
        (throw (SQLFeatureNotSupportedException.)))
      Closeable
      (close [_]
        (vreset! closed? true)
        (run! internal/interrupt-thread! threads)))))

(comment

  (set-agent-send-off-executor!
    (java.util.concurrent.Executors/newVirtualThreadPerTaskExecutor))

  (def logs (agent ""))

  (alter-var-root #'jdbc/get-datasource (fn [_] (constantly :mock/datasource)))
  (alter-var-root #'jdbc/get-connection (fn [_] (constantly (reify java.sql.Connection
                                                              ;(setAutoCommit [_ _] nil)
                                                              ;(setReadOnly [_ _] nil)
                                                              (isClosed [_] false)
                                                              (isValid [_ _] true)
                                                              (unwrap [_ _] :new)
                                                              (close [_] nil)))))

  (let [ds (datasource nil {:maximum-pool-size 5
                            :max-lifetime 5000
                            :idle-timeout 2000

                            :log-fn (fn [& args] (send-off logs (fn [s] (apply println args) s)) nil)})

        concurrency 100]
    (println "START" (java.time.Instant/now))
    (->> #(future
            (let [^long ms (inc (rand-int 2000))]
              (with-open [^java.sql.Connection conn (.getConnection ds)]
                (Thread/sleep ms)
                (send-off logs (fn [s] (doto s (println "done - slept for" ms \- (java.time.Instant/now)))))
                (println "--releasing")
                )))
         (repeatedly concurrency)
         dorun
         time)
    (.close ^Closeable ds)
    )
  (require '[criterium.core :refer [quick-bench]])

  (let [ds  (datasource nil {:maximum-pool-size 5
                             :connection-timeout -1})]
    (quick-bench ;; Execution time mean : 2.906309 ns
      #(with-open [conn (.getConnection ds)]
         (send-off logs println (.unwrap conn nil))
         ;(println (.unwrap conn nil))
         )))
  )