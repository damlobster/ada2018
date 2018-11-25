

[Updated 23.11.2018]

# **Environment news in the world**

# Abstract
Concerns about environmental degradation is taking a growing place in the collective consciousness. Global media plays an important role in reporting environmental news to population, being for reporting natural disaster that causes important damage to population, or revealing the douteuse practice of a company, politic views on environment etc and can have an impact at a large scale...

We would like to analyse the environment news coverage in the world. 

More precisely, we would like to see what are the main actors implied in environment, how they are connected to each other .

For this analysis, we are going to use the GDELT v2.0 dataset which contains information from the world's news media.


# Research questions
***Temporal Approach***
- What is the evolution in time of the worldwide news coverage (proportion and tone) of environment-related events?
- What are the major recent events related to environment?

***Spatial Approach***
- What is the proportion and the tone of environment-related news by countries? (Case-Study of European countries)
- Is there any relation between the economy and the news coverage of environment in countries?
- What is the attention of regions of the world on the disasters occurring in a specific country?

***Major actors***
- What are the major actors (person, organisation, country) involved in environment-related events?
- How strong is the link between them?




# Dataset
We are going to use the GDELT (Global Database for Events, Languages and Tones) 2.0 dataset from the cluster which contains open data from the world's news media with local events, reactions and emotions of even the most remote corners of the world from 2015 to 2017. It is based on news reports from a variety of international news sources encoded using the Tabari system for events.

The data has been well parsed and the dataset is codified with [CAMEO](https://www.gdeltproject.org/data/documentation/CAMEO.Manual.1.1b3.pdf) which will enable us to make some precise analysis.

GDELT has highly been used to make maps and link graphs according to the several examples shown by the official site. Therefore, we would like to have a map of the world, a heatmap of the ecological scandal according to their importance. Then we could select one big scandal and see the links with other events in the world. Finally, we can see the repercussions of this scandal on the country with visual facts about the economy, pollution etc… A general analysis of these repercussions all over the world will be available.

We will use the following fields of the 

**[GlobalKnowledgeGraph (GKG) V2.1 :](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf)**

- ```GKGRECORDID```: unique identifier of the GKG record
- ```V2.1DATE```: This is the date in YYYYMMDD format on which the news media used to construct this GKG file was published
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