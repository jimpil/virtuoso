(ns virtuoso.core
  (:require [next.jdbc :as jdbc]
            [virtuoso.internal.impl :as internal])
  (:import  [java.io Closeable]
            [java.sql SQLException SQLFeatureNotSupportedException]
            [javax.sql DataSource]
            [java.util.concurrent LinkedTransferQueue TimeUnit]
            [virtuoso.internal ReusableConnection]))

(defn datasource
  "Returns a `java.sql.DataSource` wrapping `(jdbc/get-datasource db-spec)`
   which reuses connections. It does this by spawning virtual-threads, all
   waiting to `TransferQueue.tryTransfer` to the first available consumer.
   In other words, there is a 'pool' of virtual-threads that are
   synchronized/coordinated via a `LinkedTransferQueue`.
   Options (can) include the following keys:

   - `:pool-size` => how many threads to start - there is no minimum/maximum as
   the 'pool' can scale down to zero w/o affecting readiness. Defaults to 10.
   Note that, physical connections are created lazily (on first consumption),
   so having N threads does not **always** mean N open connections. In  fact,
   it doesn't matter how many threads you start. Unless there are actual consumers,
   no physical connections will be opened.

   - `:connection-timeout` => how long (in ms) the returned DataSource should wait
   for a (reusable) connection. Can be zero/negative to disable (i.e. wait forever).
   Defaults to 30,000 (i.e. 30 seconds).

   - `:idle-timeout` => how long (in ms) a (reusable) connection is allowed to stay idle for,
   which translates to, how long a thread should stay blocked trying to transfer a
   (reusable) connection to some consumer. Defaults to 600,000 (i.e. 10 minutes).

   - `:max-lifetime` => how long (in ms) a (reusable) connection is allowed to live for.
   Defaults to 1,800,000 (i.e. 30 minutes). Note that, the 'timer' doesn't starts ticking
   until the underlying (physical) connection is realised (i.e. created).

   - `:validation-timeout` => how long (in ms) a validation check (per `Connection.isValid(int)`)
   is allowed to go for (before returning false), which can only be triggered via `:idle-timeout`.
   In other words, validation checks are performed only after idle timeouts. Defaults to 5000.

   - `:log-fn` => a 2-arg (message/data)/non-blocking/nil-returning function to be called
   every time something interesting (reusing/replenishing/closing/timeouts etc) happens.
   Defaults to `(constantly nil)`.

   The premise is that it is simpler/cheaper to simply block
   virtual-threads, than trying to maintain an auto-scalable/shrinkable
   pool, and the numbers seem to back that up.



   This DataSource is able
   of giving you back a physical `Connection` in just over 3ns,
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
  [db-spec {:keys [pool-size ^long connection-timeout log-fn]
            :or {connection-timeout 30000
                 pool-size 10
                 log-fn internal/noop}
            :as opts}]
  (let [ds      (jdbc/get-datasource db-spec)
        ltq     (LinkedTransferQueue.)
        closed? (volatile! false)
        threads (mapv
                  (fn [i] (internal/thread-conn ds i closed? ltq opts))
                  (range pool-size))
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
            @closed?
            (throw
              (SQLException. "Datasource has been closed!"))

            (nil? conn)
            (do  ;; connection-timeout
              (log-fn "Creating non-reusable connection (slow path)!" {})
              (jdbc/get-connection db-spec opts))

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