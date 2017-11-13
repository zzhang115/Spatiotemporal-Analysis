# Project 2 - Spatiotemporal Analysis with MapReduce

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/courses/cs686/projects/project-2.html

# Deliverables

The project specification defines several questions that you will answer with MapReduce jobs. You should edit this document (README.md) with your answers as you find them, including figures, references, etc. This will also serve as a way of tracking your progress through the milestones.

## Deliverable I
`(All of my MapReduce job based on 30% dataset and ran on my local machine)`<br><br>
### Warm-up

#### [0.5 pt] How many records are in the dataset?<br>
* number of records: 108,000,000;<br>
* time: 14 mins;<br>

#### [0.5 pt] Are there any Geohashes that have snow depths greater than zero for the entire year? List some of the top Geohashes.<br>
>There are totally 3 Geohashes that have snow depths greater than zero every month for the entire year.<br>
SnowDepth: 590.09<br>![](/images/snow0.png)<br><br>
SnowDepth: 331.10<br>![](/images/snow1.png)<br><br>
SnowDepth: 204.93<br>![](/images/snow2.png)<br><br>
* time: 24 mins;<br>

#### [0.5 pt] When and where was the hottest temperature observed in the dataset? Is it an anomaly?<br>

Aug 23 2015 Temp: 89.52ºF<br>![](/images/hotest0.png)<br><br>
Aug 22 2015 Temp: 89.49ºF<br>![](/images/hotest1.png)<br><br>
Apr 27 2015 Temp: 89.45ºF<br>![](/images/hotest2.png)<br><br>
* time: 46 mins;<br> 

### Analysis
#### [1 pt] Where are you most likely to be struck by lightning? Use a precision of 4 Geohash characters and provide the top 3 locations.<br>
9g3m	Lighting Times: 677.0<br>![](/images/lighting0.png)<br>
9g0g	Lighting Times: 711.0<br>![](/images/lighting1.png)<br><br>
9g3y	Lighting Times: 713.0<br>![](/images/lighting2.png)<br><br>
* time: 30 mins;<br> 

#### [1.5 pt] What is the driest month in the bay area? This should include a histogram with data from each month. (Note: how did you determine what data points are in the bay area?)<br>
Bay area latitute range: 37.265 ~ 38.505<br>
Bay area longitute range: -123.041 ~ -121.624<br>
Image from wikipedia:<br> ![](/images/bayarewike.png)<br><br>
Bay area range by geohash:<br> ![](/images/bayarea.png)<br><br>
Monthly Humidity:<br> ![](/images/humidity.png)<br><br>
So the driest month is Mar<br><br>
* time: 42 mins;<br> 

#### [3 pt] After graduating from USF, you found a startup that aims to provide personalized travel itineraries using big data analysis. Given your own personal preferences, build a plan for a year of travel across 5 locations. Or, in other words: pick 5 regions. What is the best time of year to visit them based on the dataset?<br>
I select region by its monthly terature and humidity, and compared to the most confortable value of both factors I searched from wikipedia:<br>
Most comfort temperature: https://en.wikipedia.org/wiki/Room_temperature<br>
Most comfort humidity: https://en.wikipedia.org/wiki/Relative_humidity<br>
Region0: Geohash: 9mz8hq3vvw5b Month: Aug Temperature: 69.85ºF Humidity: 54.92<br>![](/images/region0.png)<br><br>
Region1: Geohash: 9mrvnncq9psp Month: Aug Temperature: 69.77ºF Humidity: 54.97<br>![](/images/region1.png)<br><br>
Region2: Geohash: 9tbm0upts7rz Month: Aug Temperature: 69.57ºF Humidity: 54.89<br>![](/images/region2.png)<br><br>
Region3: Geohash: 9mzsxkvt3xkp Month: Aug Temperature: 69.44ºF Humidity: 55.09<br>![](/images/region3.png)<br><br>
Region4: Geohash: 9sk0q7w7mk20 Month: Aug Temperature: 70.57ºF Humidity: 55.41<br>![](/images/region4.png)<br><br>
* time: 36 mins;<br> 


#### [3 pt] Your travel startup is so successful that you move on to green energy; here, you want to help power companies plan out the locations of solar and wind farms across North America. Write a MapReduce job that locates the top 3 places for solar and wind farms, as well as a combination of both (solar + wind farm). You will report a total of 9 Geohashes as well as their relevant attributes (for example, cloud cover and wind speeds).<br>

Best top3 solar locations:<br><br>
Location0: Geohash: d4g34rxuxcrf windSpeed: 33.75 cloudCover: 8.81<br>![](/images/solar0.png)<br><br>
Location1: Geohash: d4f8x4ryxfpy windSpeed: 28.30 cloudCover: 8.81<br>![](/images/solar1.png)<br><br>
Location2: Geohash: d4u024xzzyp6 windSpeed: 31.03 cloudCover: 8.82<br>![](/images/solar2.png)<br><br>

Best top3 wind locations:<br><br>
Location0: Geohash: b392r64gpbpz windSpeed: 91.81 cloudCover: 31.22<br>![](/images/wind0.png)<br><br>
Location1: Geohash: b33pgbbfrcxv windSpeed: 91.76 cloudCover: 31.22<br>![](/images/wind1.png)<br><br>
Location2: Geohash: b33p2txurcru windSpeed: 91.68 cloudCover: 31.22<br>![](/images/wind2.png)<br><br>

Best top3 wind and solar locations:<br><br>
Location0: Geohash: d7mf7qxtqx0p score: 74.33 windSpeed: 86.02 cloudCover: 11.68<br>![](/images/solarwind0.png)<br><br>
Location1: Geohash: d7mg1mu0rm8p score: 73.36 windSpeed: 84.61 cloudCover: 11.24<br>![](/images/solarwind1.png)<br><br>
Location2: Geohash: d7mg5fny3ju0 score: 73.08 windSpeed: 84.90 cloudCover: 11.82<br>![](/images/solarwind2.png)<br><br>
* time: 50 mins;<br> 


If you’d like to do some data fusion to answer this question, the maps here and here might be helpful.
#### [3 pt] Given a Geohash prefix, create a climate chart for the region. This includes high, low, and average temperatures, as well as monthly average rainfall (precipitation). Here’s a (poor quality) script that will generate this for you.<br>
position1:<br><br>
![](/images/9e.png)<br><br>
* time: 25 mins;<br> 
position2:<br><br>
![](/images/9mzs.png)<br><br>
* time: 23 mins;<br> 

## Deliverable II

Responses go here.
