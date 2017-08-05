# tables

Very simple schemaless document database. You can add few indexes, but based only on values of top level keys.

It is useful to build "tables" for search over different columns.

## Usage

Create empty table:

```
    Table0=tables:init().
```

Or you may add few indexes on creation:

```
    Table0=tables:init(#{index=>[a,b]}).
```

Add some objects:

```
    {ok,ObjectID1,Table1}=tables:insert(#{n=>element1,a=>1,b=>2,x=>100}, Table0),
    {ok,ObjectID2,Table2}=tables:insert(#{n=>element2,a=>1,b=>4,x=>200}, Table1).
```

Now you can get it by ID:

```
    tables:get(ObjectID1,Table2).    
```

And also you can lookup it by any fields:

```
    tables:lookup(n,element1,Table2).
    tables:lookup(a,1,Table2).
    tables:lookup(x,100,Table2).
    tables:lookup(b,4,Table2).
```

You can add indexes later:

```
    Table3=tables:addindex([n,x],Table2),
```

Now fields n and x will be indexed too.


