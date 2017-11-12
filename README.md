# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I

For this project, you’ll produce several small MapReduce jobs. Each of the tasks below can be broken up into several jobs, or you can combine some of them. As usual, some aspects of these questions are left up to your own interpretation. There are no right/wrong answers, but you should be able to justify your approach.
Important: many of the questions are answered with a time or location. You should also output relevant feature values to back up your answer. For instance, if I ask you which city has the most fast food restaurants per capita, you shouldn’t just say “Paducah, Kentucky.” You should also output how many restaurants are there, the population, etc. Please also include how long the MapReduce job ran for.
`(All of my MapReduce job based on 30% dataset and ran on my local machine)`<br>
### Warm-up

#### [0.5 pt] How many records are in the dataset?<br>
* number of records: 108,000,000;<br>
* time: 14 mins;<br>

#### [0.5 pt] Are there any Geohashes that have snow depths greater than zero for the entire year? List some of the top Geohashes.<br>
>There are totally 3 Geohashes that have snow depths greater than zero every month for the entire year.<br>
`Geohash        SnowDepth(Total depth for the entire year)`
* c1gyqex11wpb	204.93
* c1p5fmbjmkrz	331.10
* c41xurr50ypb	590.09

#### [0.5 pt] When and where was the hottest temperature observed in the dataset? Is it an anomaly?<br>

### Analysis
#### [1 pt] Where are you most likely to be struck by lightning? Use a precision of 4 Geohash characters and provide the top 3 locations.<br>
#### [1.5 pt] What is the driest month in the bay area? This should include a histogram with data from each month. (Note: how did you determine what data points are in the bay area?)<br>
#### [3 pt] After graduating from USF, you found a startup that aims to provide personalized travel itineraries using big data analysis. Given your own personal preferences, build a plan for a year of travel across 5 locations. Or, in other words: pick 5 regions. What is the best time of year to visit them based on the dataset?<br>
#### [3 pt] Your travel startup is so successful that you move on to green energy; here, you want to help power companies plan out the locations of solar and wind farms across North America. Write a MapReduce job that locates the top 3 places for solar and wind farms, as well as a combination of both (solar + wind farm). You will report a total of 9 Geohashes as well as their relevant attributes (for example, cloud cover and wind speeds).<br>
If you’d like to do some data fusion to answer this question, the maps here and here might be helpful.
#### [3 pt] Given a Geohash prefix, create a climate chart for the region. This includes high, low, and average temperatures, as well as monthly average rainfall (precipitation). Here’s a (poor quality) script that will generate this for you.<br>

## Deliverable II

Responses go here.
