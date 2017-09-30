#The problem:
-The goal is to produce a template for a list of 10,000 users which will be used for a mass email campaign.
-Please assume that all of these datasets are large enough that they wouldn't fit into memory on one machine.
-Please use Scala and Spark RDD (not DataFrames or SQL) as we use Spark and Scala in production for our Data Science and Machine Learning applications.
-Although the data is small in this exercise, we want you to assume that data is large and as such you should use a distributed data processing platform (Spark).
-This exercise is designed to test your design and coding abilities for preparing and transforming a large data set.

#The list of 10,000 users should be generated according to the following instructions:
-Parse the files as necessary
-The top 10,000 customers will be determined by the total amount of money they have spent in the year 2016
-We cannot reach out to a customer via an email address that is blacklisted. However, if a customer has multiple email addresses, and some are on the blacklist while others are not, we want to contact them via all their email addresses that aren't in the blacklist
-Save the campaign in a file with following structure: CustomerId, Customer full name, email list, total transaction amount
