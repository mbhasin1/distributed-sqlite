# Distributed SQLite Database

## Introduction

Distributed SQLite is a distributed transaction layer built on top of SQLite, which is an in-memory database stored on a .db file. The layer is written in Go and utilizes a [SQLite driver](https://github.com/mattn/go-sqlite3) that conforms to the built-in database/sql interface.

## System information

We created a Users table which already exists on the database with the following schema:

```
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, email VARCHAR);
```

**The system has two partitions and each partition can support upto two active nodes. At least one node must be active in each partition for the system to be available. The system can be accessed by interacting with the leader node.** Partitioning is implemented by hashing the primary key (id) and the active nodes in each partition are guaranteed to be consistent with each other. All queries are executed on the leader node instance, which sends them to the approriate followers and outputs the response.

## Usage

The leader and each follower must be spun up on separate terminals. As mentioned above, **at least one node in each partition must be active.**

1. Spin up leader node on one terminal
``` 
go run leader.go localhost:8080 
```

2. Spin up one follower node in partition #1
``` 
go run follower.go localhost:8080 db1.db
```

3. Spin up one follower node in partition #2
``` 
go run follower.go localhost:8080 db2.db
```

4. Optional: Spin up an additional follower in partition #1
``` 
go run follower.go localhost:8080 db3.db
```

5. Optional: Spin up an additional follower in partition #2
``` 
go run follower.go localhost:8080 db4.db
```

6. Execute any query(s). For a singular query, write the query and hit enter. For multiple queries, please write them one after another and then only hit enter. **For simplicity, please use the existing schema defined above (table name is Users).**

Example queries:
```
select * from Users;
```
```
insert into Users values (2, 'Charlie', 'charlie@sjsu.edu');
```
```
select * from Users where id=2;
```
```
insert into Users values (3, 'Dave', 'dave@sjsu.edu'); insert into Users values (4, 'Erin', 'erin@sjsu.edu');
```

7. For output of queries, view leader's logs in its terminal, or open the output file (out.txt). Additionally, view the database contents via [SQLite's Database Viewer](https://sqliteviewer.app) by providing the corresponding .db file.


## Testing the system

Availability can be tested by killing one follower instance (control + C to end terminal) assuming there are two active nodes in the partition. The system will be still be available. To test consistency, bring back up that node that was just brought down **using the same command used to spin it up**. The nodes in that partition will be consistent with each other. Two-phase commit protocol is strictly enforced on each write query. 2PC can be tested by executing a list of queries in which one query causes an error. This transaction will not be committed.

[**Here is an example demo of our system.**](https://youtu.be/yA76lHdNX7g)

## Team Organization

All the members of our group have contributed greatly to the progress of this project so far. We have been meeting in-person weekly for 3-5 hours and have been using pair programming for most of the work on this project.