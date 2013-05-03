# turntable

Turntable is a metrics tool for running SQL queries on databases at specific
times and storing the results in a table. It features a data rendering API
compatible with graphite and is well suited to graphing data. When it runs
a query, it passes the current time to the query as a timestamp, and any
`?::timestamp`s in the sql are replaced with the current time.

It is written specifically for postgresql but may work for other databases.
If so, this compatibility is not guaranteed forever. Contributions to fix
any problems it has working with other databases will be accepted, of course.

## Usage

Turntable is a web service, but is also a library. It is designed for you to
write code to start it with your own configuration and such. There is a default
example server that can be run with `lein ring server-headless`.

### Configuration

You'll need to add your database info to `sample.config.clj` (or, if not using
the default server, you can get this config and pass it in however you like).
The configuration is a map that looks like:

```clojure
{:servers {"db.foo.com:5432/dbname" {:subprotocol "postgresql",
                                      :user "user"
                                      :password "password"}}
 :query-file "queries.clj"}
```

`:servers` here is a map of database names to a jdbc map. You'll notice that
we didn't include the `:subname` key in the map, yet jdbc requires it. If
the database name is a URL (as it is in this case), then the `:subname` is
set automatically. This is to help prevent duplication in your configs.

`:query-file` is the path to where you'd like to store queries. They are
stored as Clojure forms in this file when you add and remove queries. The
way we store queries may change in the future, but for now it is a simple
`.clj` file.

### API

Turntable is a web service, so everything you do is via the API. The API
lets you add queries, remove queries, stage (test) queries, get info about
queries, and list all queries grouped by database. Here are some examples
using httpie.

```
http POST http://localhost:3000/add name==aquery sql=="some sql that preferrably results in a field with a single number" db=="db.foo.com:5432/dbname" period=="{:minute [0 15 30 45]"} backfill==1234567
```

`/add` requires that you give it a name for the query, sql to run, the db to
run it on, and a [chronicle](https://github.com/flatland/chronicle) spec
telling it at what times to run the query. If you pass `period` as `{}`, it
runs every minute of every day of every month of every year, etc. Chronicle
supports complex time settings like cron, so take a look at it for complex
needs.

The `backfill` argument is a feature that lets you specify a time in seconds
since epoch and turntable will run the query from then up until now at times
matched by the `period` map, passing the query those times. This is a useful
way to get data from long before you thought to start running the query. Be
careful about queries that take a long time to run though, it might take a
very long time to backfill all of the data if you're looking back really
far. At the moment, you can only backfill when you add a query, so if the
server dies in the middle of it, you lose your chance. An API endpoint for
backfilling after the fact is coming soon, and will require you to specfiy
a time range to backfill for.

All of the results captured when the query runs are stored in a table in
the same database that the query runs in. The table is the name of the
query and each saved snapshot includes: time it took to run the query
in milliseconds, time when it started, time when it finished, and the
time passed in by turntable to seed. This seed time will be the time
when the query started for non-backfill query runs and the a past time
during backfill runs

```
http POST http://localhost:3000/remove name==aquery
```

This one is simple. It removes the query and stops it from running. All
of the saved data remains in the table.

```
http POST http://localhost:3000/stage db=="db.foo.com:5432/dbname" sql=="some sql"
```

This is a simple way to test a query before committing to adding it, so
that you can make sure that it runs properly through turntable. It returns
the results or an exception if one occurs. Even if your SQL runs in
psql, you want to make sure it runs through stage before adding your
query. For example, sometimes a query will throw an exception if you
forget to cast one of your `?`s to `timestamp`.

```
http http://localhost:3000/get name==aquery
```

This returns information about the query such as name, database it runs
on, and the period map for when it is supposed to run.

```
http http://localhost:3000/queries
```

This lists all queries and returns a hash like

```json
{
  "db.foo.com:5432/dbname": {
    "aquery": "some sql"
  }
}
```

That's about it. There is also a WIP admin interface in `resources/` that
is JS-based. Improvements are being made to it as a priority, so look for
it to get more usable soon.

## License

Distributed under the Eclipse Public License, the same as Clojure.
