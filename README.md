Hadoop MapReduce using AWS

Steps:
1. Create a maven project.
2. Create Map and Reduce class.
3. Update dependencies in pom.xml.
4. Run the project as Java application.
5. Test the application locally.
6. Export the project as a Runnable JAR file.
7. Create a Amazon S3 bucket and upload all input files and JAR's to it.
8. Create a cluster in Amazon EMR by selecting required EC2 instance type and number of instances. 1 will be a master instance and the others will be slave instances.
9. After instances are running, create a task by selecting the appropriate JAR, input files and ouput folder that were uploaded previously in S3 bucket.
10. After the task is completed, output will be written to the mentioned output folder in S3 bucket.
11. Download the output files if required.

Tasks Completed:
1. Counting number of occurrences of each word in the given input file.
2. Counting number of occurrences of each double word sequence in the given input file.
3. Counting number of occurrences of individual words in the given input file that are present in another txt file using Distributed Caching feature in AWS.
