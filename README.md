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
SnowDepth: 590.09
![](/images/snow0.png)<br>
SnowDepth: 331.10
![](/images/snow1.png)<br>
SnowDepth: 204.93
![](/images/snow2.png)<br>
* time: 24 mins;<br>

#### [0.5 pt] When and where was the hottest temperature observed in the dataset? Is it an anomaly?<br>

Aug 23 2015 Temp: 89.52ºF
![](/images/hotest0.png)<br>
Aug 22 2015 Temp: 89.49ºF
![](/images/hotest1.png)<br>
Apr 27 2015 Temp: 89.45ºF
![](/images/hotest2.png)<br>
* time: 46 mins;<br> 

### Analysis
#### [1 pt] Where are you most likely to be struck by lightning? Use a precision of 4 Geohash characters and provide the top 3 locations.<br>
9g3m	Lighting Times: 677.0
![](/images/lighting0.png)<br>
9g0g	Lighting Times: 711.0
![](/images/lighting1.png)<br>
9g3y	Lighting Times: 713.0
![](/images/lighting2.png)<br>

#### [1.5 pt] What is the driest month in the bay area? This should include a histogram with data from each month. (Note: how did you determine what data points are in the bay area?)<br>
Bay area latitute range: 37.265 ~ 38.505<br>
Bay area longitute range: -123.041 ~ -121.624<br>
Image from wikipedia:<br> ![](/images/bayarewike.png)<br>
Bay area range by geohash:<br> ![](/images/bayarea.png)<br>
<br>
Monthly Humidity:<br>
![](/images/humidity.png)<br>
#### [3 pt] After graduating from USF, you found a startup that aims to provide personalized travel itineraries using big data analysis. Given your own personal preferences, build a plan for a year of travel across 5 locations. Or, in other words: pick 5 regions. What is the best time of year to visit them based on the dataset?<br>

Region0: Geohash: 9mz8hq3vvw5b Month: Aug Temperature: 69.85ºF Humidity: 54.92<br>![](/images/region0.png)<br><br>
Region1: Geohash: 9mrvnncq9psp Month: Aug Temperature: 69.77ºF Humidity: 54.97<br>![](/images/region1.png)<br><br>
Region2: Geohash: 9tbm0upts7rz Month: Aug Temperature: 69.57ºF Humidity: 54.89<br>![](/images/region2.png)<br><br>
Region3: Geohash: 9mzsxkvt3xkp Month: Aug Temperature: 69.44ºF Humidity: 55.09<br>![](/images/region3.png)<br><br>
Region4: Geohash: 9sk0q7w7mk20 Month: Aug Temperature: 70.57ºF Humidity: 55.41<br>![](/images/region4.png)<br><br>

9qp149kn2e00 7	54.483870967741936 69.4483161035156 0.017265362744079686
9mp98crhtpsp 7	55.666666666666664 69.66265177018232 0.01694047254717891
9mxpvc16v9up 7	54.31578947368421 69.74930347193686 0.016021570359890415
9sk0q7w7mk20 5	55.411764705882355 70.57787639763325 0.015742008125089205
9mzmn28hh2rz 7	54.67741935483871 69.45985384545122 0.013581476275707282
9mrzpu9heg5b 7	55.32258064516129 69.47029610351564 0.013432301160501346
9mzdn5cv0u2p 7	54.885714285714286 70.62172724637276 0.010959739883247168
9t2pf6ch8e00 7	54.63333333333333 69.74523677018232 0.01030614137834787
9mzsxkvt3xkp 7	55.096774193548384 69.44255126480596 0.009723084151703712
9w00h5teqfh0 7	54.714285714285715 70.29457896065861 0.009403076061356774
9tbkp9fs6r7z 7	54.74285714285714 69.7059849606585 0.008875539523060472
9tbm0upts7rz 7	54.892857142857146 69.57050896065846 0.008083638224359637
9t2ne52xteb0 7	54.78378378378378 69.73758880621835 0.007679935270941743
9mrvnncq9psp 7	54.97142857142857 69.77796381780138 0.0036914259794608916
9mz8hq3vvw5b 7	54.925 69.85724510351565 0.0034029920276985315

#### [3 pt] Your travel startup is so successful that you move on to green energy; here, you want to help power companies plan out the locations of solar and wind farms across North America. Write a MapReduce job that locates the top 3 places for solar and wind farms, as well as a combination of both (solar + wind farm). You will report a total of 9 Geohashes as well as their relevant attributes (for example, cloud cover and wind speeds).<br>

Best top3 solar locations:<br><br>
Location0: Geohash: d4u024xzzyp6 windSpeed: 33.75 cloudCover: 8.81<br>![](/images/solar0.png)<br><br>
Location1: Geohash: d4f8x4ryxfpy windSpeed: 28.30 cloudCover: 8.81<br>![](/images/solar1.png)<br><br>
Location2: Geohash: d4g34rxuxcrf windSpeed: 31.03 cloudCover: 8.82<br>![](/images/solar2.png)<br><br>

Best top3 wind locations:<br><br>
Location0: Geohash: b33p2txurcru windSpeed: 91.81 cloudCover: 31.22<br>![](/images/wind0.png)<br><br>
Location1: Geohash: b33pgbbfrcxv windSpeed: 91.76 cloudCover: 31.22<br>![](/images/wind1.png)<br><br>
Location2: Geohash: b392r64gpbpz windSpeed: 91.68 cloudCover: 31.22<br>![](/images/wind2.png)<br><br>

Best top3 wind and solar locations:<br><br>
Location0: Geohash: d7mf7qxtqx0p score: 74.33 windSpeed: 86.02 cloudCover: 11.68<br>![](/images/solarwind0.png)<br><br>
Location1: Geohash: d7mg1mu0rm8p score: 73.36 windSpeed: 84.61 cloudCover: 11.24<br>![](/images/solarwind1.png)<br><br>
Location2: Geohash: d7mg5fny3ju0 score: 73.08 windSpeed: 84.90 cloudCover: 11.82<br>![](/images/solarwind2.png)<br><br>


Best top3 wind locations(b8y3qv0w5tpb	 windSpeed: 91.68159203980099 cloudCover: 31.220542120339214)
Best top3 wind locations(bbg9cjph0zzz	 windSpeed: 91.76666666666667 cloudCover: 31.090935619640224)
Best top3 wind locations(ff4q0qxpvtfp	 windSpeed: 91.8177570093458 cloudCover: 24.734036081061706)

Best top3 solar locations(d5hcrwzdk8bp	 windSpeed: 31.030150753768844 cloudCover: 8.822445289180521)
Best top3 solar locations(d5j29gczt0pb	 windSpeed: 28.305 cloudCover: 8.818374460993303)
Best top3 solar locations(d551yv8hrdup	 windSpeed: 33.754901960784316 cloudCover: 8.81687091941489)

Best top3 wind and solar locations(73.0800333495097 d7mgcc7387u0	 windSpeed: 84.90602409638554 cloudCover: 11.82599074687583)
Best top3 wind and solar locations(73.36739407720549 d7msh8muq27b	 windSpeed: 84.61574074074075 cloudCover: 11.248346663535258)
Best top3 wind and solar locations(74.33902484180402 d7mkpx2sbfs0	 windSpeed: 86.02830188679245 cloudCover: 11.689277044988431)


If you’d like to do some data fusion to answer this question, the maps here and here might be helpful.
#### [3 pt] Given a Geohash prefix, create a climate chart for the region. This includes high, low, and average temperatures, as well as monthly average rainfall (precipitation). Here’s a (poor quality) script that will generate this for you.<br>
9e
01	320.93088 270.66653 30 297.045
02	321.85590 269.29870 25 297.143
03	324.72710 273.11950 25 297.684
04	327.48755 275.69708 29 299.053
05	328.35962 275.74610 34 299.776
06	328.28564 280.97998 42 300.202
07	327.59595 278.61230 46 300.750
08	329.07056 277.55640 48 301.538
09	327.36328 277.06080 49 301.424
10	325.36450 274.46167 43 300.909
11	322.37085 273.94458 39 299.865
12	320.88922 270.73184 31 298.557

9mzs
01	297.80646 270.23587 14 285.208
02	301.90436 277.38217 11 290.257
03	311.68375 277.45980 12 294.925
04	315.33325 276.96393 10 295.521
05	318.26465 279.60687 14 298.496
06	321.82373 283.56958 22 306.608
07	321.10913 288.70776 31 306.843
08	325.25708 292.94775 37 309.219
09	321.93042 290.03590 31 306.492
10	314.71240 279.46060 24 297.651
11	305.70288 267.64900 10 286.684
12	297.51000 266.96760 7 281.068


## Deliverable II

Responses go here.
