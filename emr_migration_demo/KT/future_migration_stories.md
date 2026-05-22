# Future Migration Stories

## Purpose

This document tracks migration improvement stories that we want to evaluate after the EMR baseline is stable.

These are not required for the first EMR baseline implementation. They are candidates for later comparison against Databricks or an optimized Spark version.

## Story 1: Legacy Compression To ZSTD

### Motivation

Some legacy EMR pipelines use older compression choices such as gzip, or they use compression settings that were chosen before modern lakehouse defaults were available.

For the migration story, we want to evaluate whether moving selected datasets or outputs to Zstandard/ZSTD improves:

```text
storage size
read performance
write performance
scan efficiency
cost
```

### Current Demo State

The current mock pipeline writes Parquet with Snappy compression:

```text
Raw data: Parquet
Current compression: Snappy
Converted data: Parquet
Final planned output: Parquet
```

So the current demo does not yet reproduce a gzip-heavy baseline.

### Future Story

Add a focused compression benchmark that compares:

```text
Snappy
Gzip
ZSTD
```

for one or more representative datasets, such as:

```text
bids
feature_log
final/brbf
```

### Why Gzip Can Be A Problem

Gzip can provide good compression, but it may create tradeoffs:

```text
slower decompression
higher CPU cost
less efficient parallel reads for plain text gzip files
older default choice in some legacy pipelines
```

For Parquet specifically, gzip is a block codec, but it can still be slower than more modern choices depending on data shape and workload.

### Why ZSTD Is A Candidate Improvement

ZSTD often provides:

```text
strong compression ratio
fast decompression
good support in modern Spark/Parquet/Delta workflows
better storage efficiency than Snappy in many cases
better read/write tradeoff than gzip in many cases
```

### Proposed Benchmark

Create a later benchmark step that writes the same selected dataset using multiple codecs:

```text
snappy
gzip
zstd
```

Then measure:

```text
output size
write runtime
read runtime
scan runtime with filters
CPU/runtime tradeoff
number of output files
```

Example benchmark shape:

```text
Read source dataset
Write with codec=snappy
Write with codec=gzip
Write with codec=zstd
Measure S3 size
Measure Spark read count runtime
Measure filtered scan runtime
Compare results
```

### Possible Implementation Options

Option 1: Side benchmark script

```text
scripts/benchmark_compression.py
```

This is simple and isolated from the main BRBF pipeline.

Option 2: Scala benchmark app

```text
com.demo.emr.CompressionBenchmarkApp
```

This keeps benchmark code in the same Scala/Maven project.

Option 3: Add codec switches to existing jobs

```text
--parquet-codec snappy|gzip|zstd
```

This is more integrated, but it complicates the first baseline.

Recommended future approach:

```text
Start with an isolated benchmark script or app.
Keep the main BRBF baseline stable.
```

### Databricks Migration Angle

The migration comparison can show:

```text
EMR baseline:
  older compression/layout choices
  Parquet files on S3

Databricks target:
  Delta Lake
  optimized file layout
  ZSTD or recommended table compression settings
  better metadata/statistics
  faster scans with optimized execution
```

### Success Criteria

The story is useful if we can show at least one of:

```text
smaller files with comparable or better read time
faster reads for similar storage size
lower storage footprint
lower scan cost
better default for final analytical outputs
```

### Current Decision

For now:

```text
Do not change Steps 08, 09, or 10.
Keep the main EMR baseline using the existing Parquet/Snappy setup.
Document gzip-to-ZSTD as a future migration story.
Add benchmark implementation after the core EMR pipeline is stable.
```

## Story 2: HyperMinHash Row-By-Row Merge To Spark Expressions

### Source Reference

This story came from the May 21, 2026 review notes and TTD laptop recording.



### Motivation

Some existing pipelines use HyperMinHash sketch merge logic inside partition-level JVM code.

The current pattern is expensive because it combines multiple overheads:

```text
row-by-row processing
mapPartitions logic
byte-array deserialization into Java/Scala objects
Union accumulator merge logic
JVM object creation
limited Photon support for the custom merge section
```

In the referenced avails-v2 pipeline, three tasks use the same Scala class and each task can take roughly 3-4 hours.

### Current Baseline Pattern

The existing logic is shaped like this:

```text
read sketch bytes
filter or slice records
enter mapPartitions
for each row:
  deserialize bytes into HyperMinHash object
  merge two HyperMinHash sketches using Union accumulator/helper method
write merged result
```

This is effectively a JVM object-processing path inside Spark.

### HyperMinHash Data Structure

The core sketch structure is an array of registers.

For this scenario, the useful working model is:

```text
one sketch = 4096 registers
register representation = array of long values
storage representation = byte array
merge operation = combine corresponding registers from two sketches
```

The expensive part is that bytes are converted into HyperMinHash objects before merge.

Future investigation:

```text
Can we decode bytes directly into a long-buffer/register representation?
Can we avoid full object deserialization?
Can we express the register merge as Spark SQL/DataFrame expressions?
Can we pack the final register result back into bytes after aggregation?
```

### What Photon Can Help Immediately

The surrounding Spark work may benefit from Photon:

```text
S3 reads
Delta/Parquet scans
filters
ID slicing such as id % 3
repartitioning or equalizing before output
writes back to S3/Delta
```

Expected benefit from only the surrounding read/write/filter work may be modest, around 10-15% depending on data size and layout.

### What Is Not Photon-Friendly Today

The main expensive section is not Photon-friendly in the current shape:

```text
mapPartitions
row-by-row object processing
HyperMinHash byte-to-object deserialization
Union accumulator merge
custom JVM helper methods
```

This is the likely giant performance opportunity. If it stays inside custom JVM logic, Photon can accelerate the edges but not the core merge.

### Proposed Optimization Idea

Convert the Union accumulator merge into Spark expressions.

Conceptual approach:

```text
decode sketch bytes into 4096 long registers
represent registers as Spark columns or an array column
group by the business id
merge corresponding registers using native aggregate expressions
pack the merged registers back into sketch bytes if downstream systems require bytes
```

One possible representation:

```text
id
register_0000
register_0001
...
register_4095
```

Then aggregate:

```text
group by id
merge register_0000
merge register_0001
...
merge register_4095
```

The exact aggregate expression depends on the HyperMinHash register merge semantics.

### Alternative Representation To Evaluate

Instead of 4096 physical columns, evaluate whether Spark array expressions can express the merge:

```text
id
registers: array<long>
```

Possible expression direction:

```text
transform
zip_with
aggregate
array_max / greatest-style register merge if semantically correct
```

This may be easier to maintain than 4096 generated columns, but it must be validated against Photon support and the exact merge logic.

### Feasibility Questions

Open questions to answer before implementation:

```text
What is the exact HyperMinHash register merge operation?
Are registers stored as 4096 longs, 4096 bytes, or another packed binary layout?
Can the byte payload be decoded into registers without constructing HyperMinHash objects?
Can Spark SQL functions decode the binary format, or do we still need a small decoding UDF?
If a decoding UDF remains, can we keep it outside the hot aggregation path?
Is 4096 columns practical for Spark planning and Databricks Query Profile?
Is array<long> more practical and still Photon-supported for the required operations?
How expensive is packing the merged registers back into bytes?
```

### Follow-Up Deserialization Question

The important technical question:

```text
If we pass an array of 4096 register values, does deserialization happen once for the sketch
or separately for each register?
```

The desired direction is:

```text
deserialize/decode once per sketch into a register representation
avoid creating full HyperMinHash objects row by row
run the merge using native Spark expressions if feasible
```

### Measurement Plan

Compare at least three versions:

```text
Baseline:
  mapPartitions + HyperMinHash object deserialization + Union accumulator

Intermediate:
  decode bytes into registers, but keep some custom logic

Optimized:
  decode to register columns or array<long>
  group by id
  merge registers using Spark expressions
  pack result back to bytes only at the end
```

Measure:

```text
runtime
executor CPU
GC time
serialization/deserialization time
shuffle read/write
spill
task skew
Query Profile operator time
Photon coverage
output sketch correctness
```

### Databricks Migration Angle

This story is valuable because it shows a more advanced migration pattern:

```text
not just run the same JAR on Databricks
not just accelerate reads and writes
convert a JVM row-by-row algorithm into a Spark-native expression plan
```

This is similar to the Geronimo-style improvement pattern:

```text
convert UDF/custom logic into Spark expressions
give the optimizer visibility
enable Photon-supported execution where possible
reduce JVM object overhead
```

The target story is a large improvement from removing the Union accumulator and row-by-row mapPartitions path, with Photon accelerating the native parts of the rewritten plan.

### Demo Fit

This does not need to be part of the first EMR BRBF baseline.

Recommended future demo approach:

```text
create a separate HyperMinHash benchmark job
generate mock sketch bytes
implement baseline mapPartitions merge
implement Spark-expression merge candidate
compare EMR baseline vs Databricks optimized run
document Query Profile and Spark UI evidence
```

### Marketplace Join Side Story

There is a related marketplace join issue:

```text
many NULLs across around 10 lookup columns
NULL-safe joins may be required
NULL-safe joins or wider hash joins can hurt performance
```

Future investigation:

```text
Can we create or use a single stable marketplace id instead of 10 nullable lookup columns?
Can we normalize lookup keys before the join?
Can we split matched/non-null join paths?
Can we reduce NULL-safe comparison cost?
Can Databricks improve the join plan once the key structure is simplified?
```

This is still TBD and should be tracked as a join-strategy optimization, separate from the HyperMinHash merge rewrite.

### Current Decision

For now:

```text
Do not add HyperMinHash logic to the current BRBF baseline.
Keep it as a future migration story.
Use it later as a focused example of replacing row-by-row JVM logic with Spark expressions.
Validate correctness carefully because sketch merge semantics must be exact.
```

## Future Story Backlog

Additional migration stories to consider later:

```text
UDFs to native Spark SQL expressions
AQE and skew join handling
manual salting removal
Parquet to Delta Lake
optimized file layout and clustering
join strategy improvements
marketplace NULL-safe join simplification
deferred derived-column computation
Photon acceleration for scans, joins, aggregations, and sorts
workflow orchestration improvements
observability and validation automation
```
