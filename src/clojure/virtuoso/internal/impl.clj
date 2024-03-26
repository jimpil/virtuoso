(ns virtuoso.internal.impl
  (:require [next.jdbc :as jdbc])
  (:import  [java.time Instant]
            [java.time.temporal ChronoUnit]
            [java.util.concurrent LinkedTransferQueue TimeUnit ScheduledThreadPoolExecutor]
            [java.sql Connection]
            [virtuoso.internal ReusableConnection]))

(defn interrupt-thread! [^Thread t] (.interrupt t))
(def noop (constantly nil))

(defn- reusable-conn
  ^ReusableConnection [db opts]
  (-> db
      (jdbc/get-connection opts)
      (delay)
      (ReusableConnection.)))

(defn- interrupt-transfer!
  [^Thread t log-fn]
  (log-fn "Max lifetime exceeded - interrupting transfer!")
  (interrupt-thread! t))

(defn ensure-max-lifetime!
  [^ScheduledThreadPoolExecutor exec ^long dlay t log-fn]
  (.schedule
    exec
    ^Runnable (partial interrupt-transfer! t log-fn)
    dlay
    TimeUnit/MILLISECONDS))

(defn thread-conn
  [ds
   thread-index
   ds-closed?
   ^LinkedTransferQueue ltq
   {:keys [max-lifetime
           idle-timeout
           validation-timeout
           log-fn]
    :or {max-lifetime 1800000
         idle-timeout 600000
         validation-timeout 5000
         log-fn noop}
    :as opts}]
  (let [validation-timeout (long (/ validation-timeout 1000))
        exec (->> (Thread/ofVirtual)
                  (.factory)
                  (ScheduledThreadPoolExecutor. 1))]
    (-> (Thread/ofVirtual)
        (.start
          ^Runnable
          (partial
            (fn vconn* [^ReusableConnection conn replenish? ds-closed?]
              (let [t (Thread/currentThread)]
                (cond
                  @ds-closed?
                  (do
                    (log-fn "Breaking recursion" {:conn conn})
                    (and (not (.isClosed conn))
                         (.close ^Connection (.unwrap conn nil))))

                  (or (Thread/interrupted) replenish?) ;; will clear the interrupt flag
                  (do
                    (log-fn "Replenishing connection" {:conn conn})
                    (and (not (.isClosed conn))
                         (.close ^Connection (.unwrap conn nil)))
                    (recur (reusable-conn ds opts) false ds-closed?))

                  :else
                  (do
                    (try
                      (.setBusy conn) ;; blocking call (per `Semaphore.acquire()`)
                      (catch InterruptedException _
                        (.interrupt t)
                        (log-fn "Interrupted while waiting for ongoing work to complete - Closing connection"
                                {:conn conn})
                        (and (not (.isClosed conn))
                             (.close ^Connection (.unwrap conn nil)))))
                    (let [created-at (.getCreatedAt conn)
                          alive      (.until created-at (Instant/now) ChronoUnit/MILLIS)
                          diff       (unchecked-subtract max-lifetime alive)]
                      (if (>= alive max-lifetime)
                        ;; replenish the connection
                        (do (log-fn "Max lifetime exceeded!"
                                    {:conn conn
                                     :exceeded-by diff
                                     :alive-for alive})
                            (recur conn true ds-closed?))
                        (let [fut (ensure-max-lifetime! exec diff t log-fn)
                              replenish?
                              (try
                                (or (Thread/interrupted) ;; bail out early (clears interrupt flag)
                                    (log-fn "Offering reusable connection"
                                            {:conn conn
                                             :alive-for alive})
                                    (and
                                      (not (.tryTransfer ltq [conn thread-index] idle-timeout TimeUnit/MILLISECONDS))
                                      (do (log-fn "Idle timeout - checking validity"
                                                  {:conn conn})
                                          ;; tranfer failed - release manually to avoid deadlock!
                                          (.close conn)
                                          (not (.isValid conn validation-timeout)))))
                                (catch InterruptedException _
                                  (.interrupt t)
                                  (log-fn "Interrupted while waiting to transfer to consumer"
                                          {:conn conn})
                                  true)
                                (finally
                                  (future-cancel fut)))]
                          (recur conn replenish? ds-closed?))))))))
            (reusable-conn ds opts)
            false
            ds-closed?)))))
