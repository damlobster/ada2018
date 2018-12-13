[Updated 25.11.2018]

# **Environment news in the world**

# Abstract
Concerns about environmental degradation is taking a growing place in the collective consciousness. Global media plays an important role in reporting environmental news to population, whether it is for reporting natural disasters that cause important damage to population and environment, or for exposing the suspicious practice of a company, political actions dealing with environment etc...

We would like to analyse the environment news coverage in the world. First, we would like to see what are the main reported actors implied in environment-related event and how they are connected to each other. Then, we would like to see what is the distribution and the tone of environmental news from a temporal and spatial view. Also it could be interesting to see what are the major events that took place recently and focus on one of these major events.

For this analysis, we are going to use the GDELT v2.0 dataset which contains information from the world's news media. More precisely, we will make high use of two metrics from GDELT, the number of mentions and the tone.


# Research questions
***Major actors***
- What are the major actors (person, organisation, country) involved in environment-related events?

- How strong is the link between them? Is there a type of actor that is more predominant than others?

***Temporal Approach***
- What is the evolution in time of the worldwide news coverage (proportion of mentions and tone) of environment-related events?

- What are the major recent events related to environment?


***Spatial Approach***
- What is the proportion of mentions and the tone used for environment-related event happening in countries (case-study of European countries)?


- Is there any relationship between the economy and the news coverage of environment in countries? Find whether there is a relationship between the previous metrics (proportion of mention and tone) that characterizes the news coverage of environment and the economy of a country.


- What is the attention of a specific region of the world on the events occuring in the world? For a given region, we would like to visualize the attention of this region on the events occuring in the entire world. This will be a flip point of view compared to the previous questions (where it was more about how the world sees a specific country).

#  Files in this repo

The [Milestone3.pynb](src/Milestone3.ipynb) explains in detail the pipeline adopted in this project. We strongly invite the reader to look at this notebook which aggregates all the information he/she will need. The [Milestone2.pynb](src/Milestone2.ipynb) was the explanations for the Milestone 2 (in case the reader wants to know retrace our progress)

In case, we provide here an architecture of the repo:

- [src/cluster](src/cluster): all the files used to create the parquets (that are used in all the other folders of src) on the cluster

- [src/tone](src/local): each subfolder contains a notebook corresponding to local computations for specific task; we have put together all the computations in the [Milestone3.pynb](src/Milestone3.ipynb)

- [src/data](src/data): data files that have been in the local computations, contains files issued from cluster computations as weel as external datasets and assets files.


# Dataset
We are going to use the GDELT (Global Database for Events, Languages and Tones) 2.0 dataset from the cluster which contains open data from the world's news media with local events, reactions and emotions of even the most remote corners of the world from 2015 to 2017. It is based on news reports from a variety of international news sources encoded using the Tabari system for events.

The data has been well parsed and the dataset is codified with [CAMEO](https://www.gdeltproject.org/data/documentation/CAMEO.Manual.1.1b3.pdf) which will enable us to make some precise analysis.

We will use the following fields of the 

**[GlobalKnowledgeGraph (GKG) V2.1 :](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf)**

- ```GKGRECORDID```: unique identifier of the GKG record
- ```V2.1DATE```: This is the date in YYYYMMDDHHMMSS format on which the news media used to construct this GKG file was published
- ```V2SOURCECOMMONNAME```: Identifier of the source of the document
- ```V2DOCUMENTIDENTIFIER```: Unique external identifier for the source document, useful to link directly with the Mentions table of GdeltEventDatabase
- ```V1COUNTS```: This is the list of Counts found in this document (eg. 3000 dead fishes)
- ```V1THEMES```: This is the list of all Themes found in the document
- ```V1LOCATIONS```: This is a list of all locations found in the text
- ```V1PERSONS```: This is a list of all person names found in the text
- ```V1ORGANIZATIONS```: This is the list of all company and organization names found in the text
- ```V1.5TONE```/Tone: This is the average “tone” of the document as a whole


**[GdeltEventDatabase V2.0: ](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)**

- ```GLOBALEVENTID```: Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset
- ```Day_DATE```: Date the event took place in YYYYMMDD format
- ```Actor1Code```: Complete raw CAMEO code for Actor1
- ```Actor1Name```: The actual name of the Actor 1
- ```Actor1CountryCode```: The 3-character CAMEO code for the country affiliation of Actor1
- ```Actor1Type1Code```: The 3-character CAMEO code of the CAMEO “type” or “role” of Actor1
- ```Actor1Type2Code```: If multiple type/role codes are specified for Actor1, this returns the second code
- ```Actor1Type3Code```: If multiple type/role codes are specified for Actor1, this returns the third code
- The fields above are repeated for ```Actor2```
- ```EventCode```:  This is the raw CAMEO action code describing the action that Actor1 performed upon Actor2
- ```GoldsteinScale```: a numeric score from -10 to +10, capturing the theoretical potential impact that type of event will have on the stability of a country
- ```NumMentions```: This is the total number of mentions of this event across all source documents during the 15 minute update in which it was first seen
- ```NumSources```: This is the total number of information sources containing one or more mentions of this event during the 15 minute update in which it was first seen.
- ```NumArticles```: This is the total number of source documents containing one or more mentions of this event during the 15 minute update in which it was first seen
- ```AvgTone```: This is the average “tone” of all documents containing one or more mentions of this event during the 15 minute update in which it was first seen
- ```Actor1Geo_Type```: specifies the geographic resolution of the match type (country, state, …)
- ```Actor1Geo_FullName```: This is the full human-readable name of the matched location
- ```Actor1Geo_CountryCode```: This is the 2-character FIPS10-4 country code for the location
- The fields above are repeated for ```Actor2``` and ```Action```

**[Mention Table: ](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)**
- ```GLOBALEVENTID```: Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset
- ```EventTimeDate```: This is the 15-minute timestamp (YYYYMMDDHHMMSS) when the event being mentioned was first recorded by GDELT.
- ```MentionTimeDate```: This is the 15-minute timestamp (YYYYMMDDHHMMSS) of the current update.
- ```MentionSourceName```: This is a human-friendly identifier of the source of the document
- ```MentionIdentifier```: Unique external identifier for the source document, useful to link directly with the GKG

To be able to filter the dataset and keep only the environment-related events we keep the items in GKG that contains one of the following tag in their V1THEMES field:
- ```ENV_[*]```: Everything related to environment (from biofuels to overfishing, solar energy, nuclear energy, deforestation...)
- ```SELF_IDENTIFIED_ENVIRON_DISASTER```: Articles where the text explicitly mentions "ecological disaster", "environmental catastrophe", etc
- ```NATURAL_DISASTER```: From floods to coldsnaps, wildfires to tornadoes
- ```MOVEMENT_ENVIRONMENTAL```: Environmental movements

We then join with the Mention table on the V2DOCUMENTIDENTIFIER mention and then we join the Mention table with GdeltEvent on GLOBALEVENTID to keep the environment-related events.

# External Datasets
We have used several external datatets to enhance our study on GDELT.

Firstly, for the flip point of our spatial approach, that is to see how a country talks about the world, we have looked for a dataset that indicates us where a web domain comes from. Indeed, GDELT gives us the source of each document (V2SOURCE COMMONNAME, in particular the web domain. For that, we have used a [geographical map of the sources](https://blog.gdeltproject.org/mapping-the-media-a-geographic-lookup-of-gdelts-sources/). This dataset geolocates approximately 190'000 domains with the associated country name and its FIPS10-4 code.

Then, we found some key information on countries to help us find any correlation between environment events and well-known metrics about a country. 


For the social development, we have used the [Human Development Index](http://hdr.undp.org/en/content/human-development-index-hdi) which is a measure of achievement in key dimensions of human development, for instance a long and healthy life, or having a decent standard of living.


Moreover, we have used the [Environmental performance index](https://epi.envirocenter.yale.edu/epi-downloads). This index ranks 180 countries on many performance indicators particularly covering environmental health. The used metrics provide a gauge at a national scale of how close countries are to achieved environmental policy goals. Eventually, we have access to this general index which gives information about the ecological involvement of the country, but also to the used metrics among which is the GDP. We recall that GDP measures the economical status of the country.

# Used Libraries 
We have used the classical libraries in Python programming: numpy, pandas, pyspark, seaborn, matplotlib.

In addition, we have used the following libraries:

- ```[Altair](https://altair-viz.github.io/)```: Table bubble plot
- ```[NetworkX](https://networkx.github.io/documentation/stable/)```: Weighted Graph 
- ```[Word Cloud](https://amueller.github.io/word_cloud/)```: Word Cloud