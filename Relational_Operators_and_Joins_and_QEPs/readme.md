
# README
Author: Lilianne Brush

## Overview

In this project, I implemented relational algebra operators, focusing on their algorithms and how they are executed in a database system. I worked with scan operators, primitive operators, hash joins, and query evaluation pipelines, deepening my understanding of database query processing.

## Part 1: Scan Operators

For this part, I implemented `KeyScan` and `KIndexScan` based on the provided `FileScan`. I was surprised by how little code was required to adapt these operators, and it helped me understand how relational operators interact with underlying data files.

## Part 2: Primitive Operators

I implemented `Selection` and `Projection`. I made sure `Selection` used `OR` for multiple predicates and that `Projection` did not remove duplicates from the result, adhering to relational algebra rules.

## Part 3: Hash Join

I read the textbook on hash join algorithms and used that knowledge to implement the partition and probing phases. I leveraged `HashtableDP` for the in-memory hash table and avoided nested loops for efficiency.

## Part 4: Query Evaluation Pipelines

In this part, I found that `Q4` and `Q5` were mismatched in the provided code, so I implemented `Q4` instead. I used the provided queries 1, 2, 3, and 5, as well as tests in `Rot.java`, to implement the remaining queries.
