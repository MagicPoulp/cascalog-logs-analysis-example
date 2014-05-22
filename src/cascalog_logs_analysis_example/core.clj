; Author: Thierry Vilmart

; Cascalog is a query language on big data that runs on Hadoop both locally or on a distributed file system.
; This program shows a simple example of cascading Cascalog queries to parse a text file.
; Two patterns are searched for the same user ID.
; The difference of counts is returned.
; Moreover, the timestamp is checked to be within an interval.

; first query: query-find-before-loss-and-return-id
; second query: query-find-after-loss-with-id
; third query: query-compare-counts

; pattern before loss:
; "3753\t33523"
; pattern after loss:
; "\t3792\t"

; The first query finds the ids with the first pattern, and counts them.
; The second query finds the ids with the second patterns, and counts them.
; The third query joins the two first queries on the parameter ?id and
; calculates the difference of counts between the two patterns.
; This is explained in Nathan Marz's presentation http://www.slideshare.net/nathanmarz/cascalog-workshop

(ns cascalog-logs-analysis-example.core
   (:use cascalog.api)
   (:use clojure.java.io)
   (require [cascalog.logic.ops :as c]))

; Finds a pattern inside a text line.
; Parentheses cannot be used in the pattern.
(defn find-pattern [pattern data]
  (re-find (re-pattern pattern) data))

; Finds a pattern inside a text line.
; The search will produce a vector of data due to the use of parentheses.
; We will only return the first element to get the entire pattern that matches.
(defn find-pattern-vector [pattern data]
  (nth (re-find (re-pattern pattern) data) 0))

(defn find-id [data]
  (re-find (re-pattern "U\\d-\\d+") data))

(defn parse-long [s]
   (Long/parseLong (re-find  #"\d+" s )))

(defn is-timestamp-in-interval [timestamp [min-timestamp max-timestamp]]
  (let [parsed-timestamp (parse-long timestamp)
        parsed-min-timestamp (parse-long min-timestamp)
        parsed-max-timestamp (parse-long max-timestamp)]
    (and (< parsed-min-timestamp parsed-timestamp)
         (< parsed-timestamp parsed-max-timestamp))))

; simple query of a pattern with output on stdout
(defn query-count-pattern [pattern source]
    (?<- (stdout) [?result ?count]
         (source :> ?line)
         (find-pattern :< pattern ?line :> ?result)
         (c/count :> ?count)
         ))

; first query
(defn query-find-before-loss-and-return-id [source pattern timestamp-interval]
    (<- [?id ?count-before-loss]
        (source :> ?line)
        (find-pattern :< (str pattern ".+") ?line :> ?line-with-pattern)
        (find-pattern :< "^\\d+" ?line :> ?time1)
        (is-timestamp-in-interval :< ?time1 timestamp-interval)
        (find-id :< ?line-with-pattern :> ?id)
        (c/count :> ?count-before-loss)
        ))

; second query
(defn query-find-after-loss-with-id [source pattern timestamp-interval]
    (<- [?id ?count-after-loss]
        (source :> ?line)
        (find-pattern-vector :< (str pattern ".+") ?line :> ?line-with-pattern)
        (find-pattern :< "^\\d+" ?line :> ?time2)
        (is-timestamp-in-interval :< ?time2 timestamp-interval)
        (find-id :< ?line-with-pattern :> ?id)
        (c/count :> ?count-after-loss)
        ))

; third query reusing that joins the results from the two first queries
(defn query-compare-counts [data-before-loss data-after-loss]
  (<- [?number-lost ?id ?count-before-loss ?count-after-loss]
      (data-before-loss :> ?id ?count-before-loss)
      (data-after-loss :> ?id ?count-after-loss) ; We join the two tables on ?id.
      (- ?count-before-loss ?count-after-loss :> ?number-lost)
      ))

(defn process-chain [source before-loss-pattern after-loss-pattern timestamp-interval]
; count users before loss
;  (?- (stdout) (query-find-before-loss-and-return-id source before-loss-pattern timestamp-interval)))

; count users before loss and users after loss. We can pipe both results to a file.
;    (?- (stdout) data-before-loss (stdout) (query-find-after-loss-with-id source after-loss-pattern data-before-loss timestamp-interval))))

; count users before count and pass it to query to count users after count
  (let [data-before-loss (query-find-before-loss-and-return-id source before-loss-pattern timestamp-interval)
        data-after-loss (query-find-after-loss-with-id source after-loss-pattern timestamp-interval)]
    (?- (stdout) (query-compare-counts data-before-loss data-after-loss))))

; how to run:
; lein run -m cascalog-logs-analysis-example.core input/data.log
; add the following to remove standard_error output:
; 2>/dev/null
(defn -main [input-dir]
  (let [source (hfs-textline input-dir)
        before-loss-pattern "\t3753\t33523\t"
        after-loss-pattern "\t3792\t(33544|33550|33547)\t",
        timestamp-interval ["20140515120050" "20140515120950"]]
    (process-chain source before-loss-pattern after-loss-pattern timestamp-interval)))

