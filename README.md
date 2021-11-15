## Project 1
- Project 1 will be a Scala console application that is retrieving data using Hive or MapReduce. Your job is to build a real-time news analyzer. This application should allow users to view the trending topics (e.g. all trending topics for news related to "politics", "tv shows", "movies", "video games", or "sports" only [choose one topic for project]).
- You must present a project proposal to trainer and be approved before proceeding with project.

### MVP:
- ALL user interaction must come purely from the console application
- Hive/MapReduce must:
    - scrap data from datasets from an API based on your topic of choice
- Your console application must:
    - query data to answer at least 6 analysis questions
    - have a login system for all users with passwords
        - 2 types of users: BASIC and ADMIN
        - Users should also be able to update username and password

### Presentations
- You will be asked to run an analysis using the console application on the day of the presentation, so be prepared to do so.
- We'll have 5-10 minutes a piece, so make sure your presentation can be covered in that time, focusing on the parts of your analysis you find most interesting.

### Technologies
- Hadoop MapReduce
- YARN
- HDFS
- Scala 2.13
- Hive
- Git + GitHub

Project Proposal:

My selection for the news analyzer will be on the topic of sports, and more specifically on the sport of hockey and the NHL.
The data will come from the NewsAPI (newsapi.org) with a free developer API key. 
This poses some annoying limitations on the number of articles that I can pull from the API at a time (100 results per API query is the main one), but I feel I can work around this.
I will also be limited to articles from the last month because of the limited license, but this still provides me with tens of thousands of articles to analyze.
I will use Hive to query the data and answer the questions.
Analysis Questions:
1. Is news about hockey increasing over the last month?
2. What team(s) have the most news about them?
3. Which of these popular players shows up in news the most: Alex Ovechkin, Sidney Crosby, or Connor McDavid?
4. How does volume of news about hockey compare to news about other popular sports in the US, basketball, football, baseball?
5. What division has the most news about it (Metro, Atlantic, Central, or Pacific)?
6. How has the NHL's newest team, the Seattle Kraken fared in terms of volume of news over the last month?
