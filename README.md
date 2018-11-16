
[Updated 16.11.2018]

# **Ecological scandals in the world through the news**

# Abstract
Concerns about environmental degradation takes a growing place in the collective consciousness. Thanks to the global media news, environmental scandals are nowadays highly exposed to the population and can have an impact at a larger scale.

We want to analyse the impact of ecological scandals on the society from a media point of view. We define ecological scandals as being punctual (in space and time) events having disastrous consequences on nature and that have been highly exposed by medias.

More precisely, we would like to see where the primary ecological scandals have taken place, how a particular scandal is connected to other scandals in the world, how important they are and most importantly what are the consequences on the society (economy, politics, social...) due to media coverage.

For this analysis, we are going to use the GDELT v2.0 dataset which contains information from  the world's news media.


# Research questions
- What is the evolution of the worldwide news coverage (number of articles and tone) of environmental related events?
- What are the major recent events related to environment?
- How much place is taking environmental related news in European countries and worldwide (in space and time)?
- What is the attention of regions of the world on the disasters occurring in a country?
- What is the link between the actors of a particular event in the world?
- Is there any relation between the economy and the news coverage of environment in countries?



# Dataset

We are going to use the GDELT (Global Database for Events, Languages and Tones) 2.0 dataset from the cluster which contains open data from the world's news media with local events, reactions and emotions of even the most remote corners of the world from 2015 to 2017. It is based on news reports from a variety of international news sources encoded using the Tabari system for events.

The data has been well parsed and the dataset is codified with CAMEO which will enable us to make some precise analysis.

GDELT has highly been used to make maps and link graphs according to the several examples shown by the official site. Therefore, we would like to have a map of the world, a heatmap of the ecological scandal according to their importance. Then we could select one big scandal and see the links with other events in the world. Finally, we can see the repercussions of this scandal on the country with visual facts about the economy, pollution etc… A general analysis of these repercussions all over the world will be available.


To be able to filter the environmental events we will use the following fields of the 

**[GlobalKnowledgeGraph (GKG) V2.1 :](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf)**

- ```GKGRECORDID```: unique identifier of the GKG record
- ```V1DATE```: This is the date in YYYYMMDD format on which the news media used to construct this GKG file was published
- ```V1COUNTS```: This is the list of Counts found in this document (eg. 3000 dead fishes)
- ```V1THEMES```: This is the list of all Themes found in the document
- ```V1LOCATIONS```: This is a list of all locations found in the text
- ```V1.5TONE```/Tone: This is the average “tone” of the document as a whole
- ```V1EVENTIDS```: comma-separated list of GlobalEventIDs from the master GDELT event stream
- ```V2GCAM```: The Global Content Analysis Measures (GCAM). Contain over 2230 type of events, we will use this field to find environmental related events.
- ```V2DOCUMENTIDENTIFIER```: unique external identifier for the source document, useful to link directly with the Mentions table of GdeltEventDatabase

**[GdeltEventDatabase V2.0: ](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)**

- ```GlobalEventID```: Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset.
- ```Day```: Date the event took place in YYYYMMDD format.
- ```Actor1Name```: The actual name of the Actor 1.
- ```Actor2Name```: The actual name of the Actor 2.
- ```EventCode```:  This is the raw CAMEO action code describing the action that Actor1 performed upon Actor2.
- ```NumArticles```: This is the total number of source documents containing one or more mentions of this event during the 15 minute update in which it was first seen. This can be used as a method of assessing the “importance” of an event.
- ```GoldsteinScale```: a numeric score from -10 to +10, capturing the theoretical potential impact that type of event will have on the stability of a country.
- ```AvgTone```: This is the average “tone” of all documents containing one or more mentions of this event during the 15 minute update in which it was first seen.
- ```Actor1&2Geo_Type```: specifies the geographic resolution of the match type (country, state, …).
- ```Actor1&2Geo_Lat&Long```: This is the centroid of the landmark for mapping.


# A list of internal milestones up until project milestone 2
|Week #|Internal Milestone|
|---|---|
|1|Discussion to refine the scope of the project. Extension of the subject to include all environmental related events (not only scandals). Redefine research questions based on TA's remarks and existing literature. Set up the tools to work on the cluster.
|2|Exploratory data analysis, cleaning of the dataset to isolate events related to environment and extract relevant information from them to answer the first research question.|
|3||



# Questions for TAs
