# Page-Rank-Analysis-WIkipedia
This Map Reduce program helps to find the Term frequency using  Inverse Document Frequency.This is implemented using hadoop(cloudera distribution)

There are three files attched with the zip

1)PageRankPR.java  - source code for the Page rank.

2)Output on Cloud - the first 100 records for the large file ran on the cluster.

3)Output on local System - the first 100 records for the small compressed file ran on the local system.

4) The output is finally stored in the output11 directory.


5) Instructions to run the jar 

hadoop jar PageRank_test.jar org.myorg.PageRankPR pagerankinput output0

**********PLease append 0 to the output directory

**********PLease run with the output directory name as output0 only as hardcoded. 
