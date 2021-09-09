# beverageAnalyticsUsingTwitterDataset

## Structured Abstract
With the improvement of computer science and network, Big Data is becoming more popular worldwide. Big Data has five properties: Volume, Variety, Velocity, Veracity and Value. In order to handle the challenges of Big Data, the distributed system is widely used with distributed frames and programming languages.

This coursework will focus on the Twitter(1% API) dataset from 07/2014 to12/2014 and analyze tweets mentioned different drink types of this dataset. We divide drinking into five types: coffee, tea, healthy drink, soft drink and alcohol. Spark is the analytics engine for Big Data processing, and Python is used to process the pre-processed data from Spark. After pre-processing, we will discuss the relationship between different language users and drink types. Another topic is the number of tweets mentioned different drink types of a day.

Finally, we will use Tableau to visualize the analysis and give some suggestions to the beverage company base on this dataset.

## Beverage analytics using the Twitter dataset
The beverage market is a vast market that is projected to reach nears $1.86 trillion by 2024. This coursework will use the Twitter (1% API) dataset to analyze drinking habits.

In this coursework, there is two central analytics about people’s drinking habits. The first one is to find the favourite type of drink in different language users. Another is to find the drinking habit at a different time of day. By this report, beverage company can find the best time to advertise for different types of drink. Furthermore, beverage company can promote a specific type of drinks for different language users to make the highest profit.

## Twitter (1% API) dataset and analyzing approaches
The Twitter (1% API) dataset is a collection of tweets available through the free Twitter API, which covers a 1% random sample of the entire Twitter stream over six months (07/2014-12/2014). There are 184 JSON files in the dataset, one for a day. Each file is about 20GB after zipping, more than 6,000,000 records of tweet with 30 dimensions. The whole dataset is more than 3TB after zipping, which is difficult to process without a distributed system. 

In this project, we used Spark as the analytics engine for Big Data processing and used Python to process the pre-processed data. Finally, we used Tableau to visualize the analysis result and gave the conclusion.

## Discussion and conclusions of the analysis

Please see the conclusionsOfAnalysis.md
