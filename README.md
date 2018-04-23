exectoy is a speed-of-light benchmark for column-vector SQL execution.

It's based off of some ideas that we've been kicking around at Cockroach for a
bit, as well as on ideas laid out in the MonetDB and MonetDB/X100 papers.

The idea is that you replace "processors" (row-by-row, CISC-y constructs that
do a whole lot per row) with "operators" (column-by-column, RISC-y constructs
that do extremely little).

For example, take the table and query:

```
CREATE TABLE t (n int, m int, o int, p int);

SELECT o FROM t WHERE m < n + 1;
```

Ordinarily, in the row-by-row model, the way this would be evaluated (sans
indexes for the purposes of discussing just the execution flow) is via the
following pseudocode:

```
next:
  for:
    row = source.next()
    if filterExpr.Eval(row):
      // return a new row containing just column o
      returnedRow row
      for col in selectedCols:
        returnedRow.append(row[col])
      return returnedRow
```

where filter is an `Expr` that's evaluated by simple interpretation - i.e.
walking the expression tree and evaluating all sub-trees until an answer is
found. We do the projection on a row-by-row basis as well, returning just the
column that was requested.


In the column-vector model, the way this would be evaluated is different.
Instead of the data being organized by row, its organized by column - and by
a batch of columns with a constant length. This toy currently uses 1024 for
the length of the batch.

The execution flow then looks a bit different. Instead of having one processor
that does the whole filtering and projection at once, we have 3 operators, each
doing a column's worth of work at once:

```
// first create an n + 1 result, for all values in the n column
projPlusIntIntConst.Next():
  batch = source.Next()

  for i < batch.n:
    outCol[i] = intCol[i] + constArg

  return batch

// then, compare the new column to the m column, putting the result into
// a selection vector: a list of the selected indexes in the column batch

selectLTIntInt.Next():
  batch = source.Next()

  for i < batch.n:
    if int1Col < int2Col:
      selectionVector.append(i)

  return batch with selectionVector

// finally, we materialize the batch, returning actual rows to the user,
// containing just the columns requested:

materialize.Next():
  batch = source.Next()

  for s < batch.n:
    i = selectionVector[i]
    returnedRow row
    for col in selectedCols:
      returnedRow.append(cols[col][i])
      yield returnedRow
```

You'll notice that there are some very specific sounding operators, like
`selectLTIntInt`. The idea is that all of these operators would be automatically
generated at compile time, for every possible combination of op (LT), type
pair (int/int), and constant vs non-constant second argument (are we comparing
against another col or a constant value). Then, the planner would have to
choose a long pipeline of these simple operators that evaluates to the correct
result, as opposed to just sending an Expr down the pipe for evaluation
per-tuple.

This strategy has a lot of benefits. Read
http://oai.cwi.nl/oai/asset/14075/14075B.pdf (Balancing Vectorized Query
Execution with Bandwidth-Optimized Storage) and
http://cidrdb.org/cidr2005/papers/P19.pdf (MonetDB/X100: Hyper-pipelineing
Query Execution) for more details, but here's the summary:

1. avoid interpretation overhead of per-tuple function calls - don't need to
   Expr.Eval()
2. for loops that are Go-native are extremely fast, can be code generated for
   every operator/type combination, and set up at plan time
3. cache behavior is good - a column at a time processing lets you load a whole
   column chunk (a vector) into memory at once and process that in sequence



----

This repo contains some sample implementations of these little operators. It
currently works only on integers and doesn't handle null values. It's super
fast, as you'd expect because it does so little:


```
BenchmarkFilterIntLessThanConstOperator-8   	 1000000	      1643 ns/op	19933.23 MB/s
BenchmarkProjPlusIntIntConst-8              	 1000000	      1038 ns/op	31541.04 MB/s
BenchmarkProjPlusIntInt-8                   	 1000000	      1369 ns/op	23919.72 MB/s
BenchmarkRenderChain-8                      	  500000	      2733 ns/op	11988.64 MB/s
BenchmarkSelectIntPlusConstLTInt-8          	  500000	      3075 ns/op	10654.75 MB/s
BenchmarkSortedDistinct-8                   	  300000	      5156 ns/op	6354.44 MB/s
```

All benchmarks are on 4 columns, which makes the numbers look better than they
are - since every operator only operates on one or two columns, you get the
rest of the columns "for free" in your benchmark, since nobody touches their
data.

----

A complete proof of concept for this needs a few more things:

1. more than just 1 datatype. Maybe 1 more fixed-size datatype and then a
   varlen datatype like string.
2. null handling. this will probably be a bitmap and a count, but i'm not sure
   how it will work and whether another set of operators will be required to
   optimize the non-null case.
3. an escape hatch for row-based operators. we'll need to build in a way for
   operators to get a row-based computation model if they need one.
4. an escape hatch for builtin functions.
5. probably a harder operator than sorted distinct, like hash join or sort.
