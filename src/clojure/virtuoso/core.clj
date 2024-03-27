(ns virtuoso.core
  (:require [next.jdbc :as jdbc]
            [virtuoso.internal.impl :as internal])
  (:import  [java.io Closeable]
            [java.sql SQLException SQLFeatureNotSupportedException]
            [javax.sql DataSource]
            [java.util.concurrent LinkedTransferQueue TimeUnit]
            [virtuoso.internal ReusableConnection]))

(defn make-datasource
  "Returns a `java.sql.DataSource` wrapping `(jdbc/get-datasource db-spec)`
   which reuses connections. It does this by spawning virtual-threads, all
   waiting to `TransferQueue.tryTransfer` to the first available consumer.
   In other words, there is a 'pool' of virtual-threads that are
   synchronized/coordinated via a `LinkedTransferQueue`.
   Options are expected per `(jdbc/get-connection this opts)`,
   but can include the following (pooling-related) keys:

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
   Defaults to 1,800,000 (i.e. 30 minutes). Note that, the 'timer' doesn't start ticking
   until the underlying (physical) connection is realised (i.e. created).

   - `:validation-timeout` => how long (in ms) a validation check (per `Connection.isValid(int)`)
   is allowed to go for (before returning false), which can only be triggered via `:idle-timeout`.
   In other words, validation checks are performed only after idle timeouts. Defaults to 5,000.

   - `:log-fn` => a 2-arg (message/data)/non-blocking/nil-returning function to be called
   every time something interesting (reusing/replenishing/closing/timeouts etc) happens.
   Defaults to `(constantly nil)`."
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
            (recur
              (unchecked-inc i)
              (do  ;; connection-timeout
                (log-fn "Creating non-reusable connection (slow path)!"
                        {:retry i})
                (jdbc/get-connection ds opts)))

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
