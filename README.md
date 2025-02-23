To construct is an Entity Resolution (ER) pipeline for deduplication of research publication datasets

1. Data Acquisition and Preparation
- extract paper ID, paper title, author names, publication venue and year of publication
- collect all the publications betweens 1995 and 2004 in VLDB and SIGMOD venues
- store extracted entries in DBLP_199_2004.csv and ACM_1995_2004.csv
2. Entity Resolution Pipeline
- Blocking: Use a blocking scheme to assign the entries to one or more buckets based on blocking
keys. Example blocking methods are structured keys, n-gram blocking, as well as hash- or sort
based blocking (e.g., by year ranges).
- Matching: For all pairs of entities in a bucket, apply a similarity function to determine if they
refer to the same entity (if above a certain similarity score threshold). Write all the matched pairs
in a file, MatchedEntities.csv. To verify the ER match quality, you need a baseline. Apply
the same similarity function on all pairs of the datasets and use the result as your baseline.
Calculate the precision, recall, and F-measure of the matches generated by your ER pipeline
compared to the baseline. Experiment with different blocking schemes, similarity functions, and
similarity score thresholds to improve match quality and reduce execution time.
- Clustering: Once you are happy with the matches, group together all the identified matches
in clusters such that all entities within a cluster refer to the same entity. Finally, resolve the
unmatched entities with a single version in the datasets and write them back to disk.
