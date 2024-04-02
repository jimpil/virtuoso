# virtuoso

## What

A pure Clojure library for fast/lightweight DB connection pooling,
based on Java virtual-threads.

## Why

The official JDBC pooling mechanism seems somewhat cumbersome/unnecessary. 
It revolves around the following interfaces (you can read more about it 
[here](https://www.progress.com/tutorials/jdbc/jdbc-jdbc-connection-pooling)):

- javax.sql.ConnectionPoolDataSource
- javax.sql.PooledConnection
- javax.sql.ConnectionEventListener

![jdbc diagram](https://d117h1jjiq768j.cloudfront.net/images/default-source/default-album/tutorialimages-album/jdbc-album/pooling.jpg?sfvrsn=1)

`virtuoso` achieves the same thing using only the standard `java.sql.Connection` interface, 
via `virtuoso.internal.ReusableConnection`, which wraps real/physical connections.

## How (it works)

There are two core concepts to understand.

### Pool of threads (not connections)

The premise is that it is simpler/cheaper to simply block virtual-threads, than trying to maintain an 
auto-scalable/shrinkable pool of connections. So, virtuoso will start N (per `:pool-size`) virtual-threads.
What these threads are doing depends entirely on demand. What they are **trying** to do is to serve 
`ReusableConnection` objects to consumers. If a thread is blocked, that is either because its `ReusableConnection`
has been acquired (and doing work), or because there are no consumers. This gives rise to some interesting semantics.

### Coordination (via *LinkedTransferQueue*) 

I mentioned that threads are waiting to serve *consumers*, but where and how are these consumers coordinated?
This is where `java.util.concurrent.LinkedTransferQueue` comes in. What really happens, is that threads block 
on `.tryTransfer()`, and this is exactly where threads/connections may (logically) become idle.  

## How (to use)

There is a single function which gives you back a `DataSource` instance.

```clj
(require '[virtuoso.core :as vt])

(def db-spec {...}) ;; as understood by `next.jdbc`
(def opts    {...}) ;; see below
(def pooled-ds (vt/make-datasource db-spec opts)) ;; <== 

;; use `pooled-ds` as your `db` arg thereafter
;; until you're done with it (i.e. your app shuts-down)

(.close ^java.io.Closeable pooled-ds)
```
### Options

The second arg to `vt/make-datasource` basically maps to the second arg of `jdbc/get-connection`, 
but it can also include all the pooling-related keys, detailed below:

- `:pool-size` => how many virtual threads to start - there is no minimum/maximum as
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
  is allowed to go for (before returning false). This can only be triggered via `:idle-timeout`.
  In other words, validation checks are performed only after idle timeouts. Defaults to 5,000.

- `:log-fn` => a 2-arg (message/data)/non-blocking/nil-returning function to be called
  every time something interesting (reusing/replenishing/closing/timeouts etc) happens.
  Defaults to `(constantly nil)`.

## PreparedStatement caching

There is none! `HikariCP` has proven that this is an anti-pattern.

## JDBC integration

There is none! `virtuoso` relies on `next.jdbc`, for creating the *initial* DataSource (from your db-spec),
and for creating physical connections from that (when needed). In other words, what `vt/make-datasource` creates 
the *initial* datasource (per `(jdbc/get-datasource db-spec)`), and returns a wrapper. Internally, that wrapper 
uses instances of `ReusableConnection`, which in-turn wrap connections created via `(jdbc/get-connection initial-ds opts)`.

## Performance

I didn't start this project with performance in mind - all I was aiming for was a *simpler* solution/architecture.
That said, I was pleasantly surprised by the numbers (1µs/cycle). Refer to `stress-test.clj` for the micro-benchmark 
(per `quick-bench!`). 

Long story short, the *happy-path*, boils down to literally 5 operations:

- `Semaphore.acquire()` - succeeds immediately (unless connection is busy)
- `LinkedTransferQueue.tryTransfer()` - succeeds immediately (unless there are no consumers)
- `Delay.get()` - succeeds immediately (unless it's the very first use of the `ReusableConnection`)
- `Connection.isValid() or .isClosed()` - depending on `:validate-on-checkout?`
- `Semaphore.release()` - via `ReusableConnection.close()`


## License

Copyright © 2024 Dimitrios Piliouras

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
