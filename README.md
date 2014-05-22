# cascalog-logs-analysis-example

Cascalog is a query language on big data that runs on Hadoop both locally or on a distributed file system.
This program shows a simple example of cascading Cascalog queries to parse a text file.
Two patterns are searched for the same user ID.
The difference of counts is returned.
Moreover, the timestamp is checked to be within an interval.


first query:  query-find-before-loss-and-return-id
second query: query-find-after-loss-with-id
third query: query-compare-counts


pattern before loss:
"3753\t33523"

pattern after loss:
"\t3792\t"


The first query finds the ids with the first pattern, and counts them.
The second query finds the ids with the second patterns, and counts them.
The third query joins the two first queries on the parameter ?id and
calculates the difference of counts between the two patterns.
This is explained in Nathan Marz's presentation http://www.slideshare.net/nathanmarz/cascalog-workshop

# To Install

Install Java 1.6 and Leiningen
fix the project.clj depending on your version of Clojure
More info on compatible versions can be found at:
https://github.com/nathanmarz/cascalog

# To run

lein deps

lein compile

lein run -m cascalog-logs-analysis-example.core input/data.log

add the following to remove standard_error output:

2>/dev/null
