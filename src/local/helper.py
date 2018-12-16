import random
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sklearn.feature_extraction.text import CountVectorizer
import networkx as nx
from collections import Counter
from community import community_louvain
import itertools

def load_countries(DATA_PATH):
    # Load the list of the countries in the world 
    countries = pd.read_csv(DATA_PATH + "countries_cleaned_europe.csv", delimiter=";")[["ISO", "Country", "Region"]]
    countries.ISO = countries.ISO.str.upper()
    countries.set_index("ISO", inplace=True)

    regions = countries.Region.unique()[:-1]

    fips_to_iso = pd.read_csv(DATA_PATH + "fips-10-4-to-iso-country-codes.csv")
    countries = countries.merge(fips_to_iso, how="left", left_index=True, right_on="ISO")
    countries.drop(columns=["Name"], inplace=True)
    countries.dropna()
    
    return countries
    
    
def join_countries(table, countries, states=None, with_iso=False):
    """Join "table" with the list of countries
    
    Arguments:
    table - the table to join, will be join on STATE column
    countries - the DataFrame containing the list of countries with their Region, Codes, ...
    states - a list of states to keep
    with_iso - add the ISO code to the output DataFrame
    """
    fields = ["Country", "FIPS", "Region"]
    if with_iso:
        fields.append("ISO")
    table = table.merge(countries[fields], how="inner", left_on="STATE", right_on="FIPS")
    table.dropna(inplace=True)
    table = table.assign(DATE=pd.to_datetime(table[["YEAR", "MONTH", "DAY"]]))
    #Filter by specific states
    if states is not None:
        table = table[table.STATE.isin(states)]
    #Drop not useful columns
    table.drop(columns=["FIPS", "YEAR", "MONTH", "DAY", "STATE"], inplace=True)
    return table


def generate_wordcloud(data, size=75, fig=None, pos=None, title=None):
    """Generate a WordCloud for the items present in data.
    
    Arguments:
    data - a pandas DataFrame with the words as index and a column Count
    size - number of words in the WordCloud
    """
    data = data.sort_values('Count', ascending=False).head(size)
    wordcloud = WordCloud(background_color="white", width=540, height=768, margin=0).generate_from_frequencies(data.to_dict()['Count'])
    if fig is None:
        plt.figure(figsize=(15, 10))
        plt.suptitle(title)
    else:
        ax = fig.add_subplot(pos)
        ax.title.set_text(title)
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis('off')
    if fig is None:
        plt.show() 

def wordcloud_combined(date):
    if date is None:
        DATA_PATH = "data/from_cluster/wordcloud_actors/"
    else:
        DATA_PATH = "data/local_generated/"
    fig = plt.figure(figsize=(12, 10))
    wordcloud_organizations(DATA_PATH, 20, date, fig, 221)
    wordcloud_persons(DATA_PATH, 20, date, fig, 222)
    wordcloud_countries(DATA_PATH, 20, date, fig, 223)
    wordcloud_cities(DATA_PATH, 20, date, fig, 224)   
    
def wordcloud_persons(DATA_PATH, nb_items, date=None, fig=None, pos=None):
    """ Generate a WordCloud for the persons for the given date
    
    Arguments:
    nb_items - number of words in the WordCloud
    """
    
    if date is not None:
        persons = pd.read_csv(DATA_PATH + "persons_final.csv")
        persons = persons[persons.Date == date]
    else:
        persons = pd.read_csv(DATA_PATH + "persons_occurences.csv")
    persons = persons.set_index("Actor")
    generate_wordcloud(persons, nb_items, fig, pos, "Persons")
    
def wordcloud_countries(DATA_PATH, nb_items, date=None, fig=None, pos=None):
    """ Generate a WordCloud for the countries for the given date
    
    Arguments:
    nb_items - number of words in the WordCloud
    """
    
    if date is not None:
        locations = pd.read_csv(DATA_PATH + "locations_final.csv")
        locations = locations[locations.Date == date]
    else:
        locations = locations = pd.read_csv(DATA_PATH + "locations_occurences.csv")
    locations = locations[locations.Actor.str.startswith('1')]
    locations.Actor = locations.Actor.str.split('#').str[1]
    locations = locations.groupby('Actor').sum()
    generate_wordcloud(locations,nb_items, fig, pos, "Countries")
    
def wordcloud_cities(DATA_PATH, nb_items, date=None, fig=None, pos=None):
    """ Generate a WordCloud for the cities for the given date
    
    Arguments:
    nb_items - number of words in the WordCloud
    """
    
    if date is not None:
        locations = pd.read_csv(DATA_PATH + "locations_final.csv")
        locations = locations[locations.Date == date]
    else:
        locations = pd.read_csv(DATA_PATH + "locations_occurences.csv")
    locations = locations[locations.Actor.str.startswith('3') | locations.Actor.str.startswith('4')]
    locations.Actor = locations.Actor.str.split('#').str[1]
    locations.Actor = locations.Actor.str.split(',').str[0]
    locations = locations.groupby('Actor').sum()
    generate_wordcloud(locations,nb_items, fig, pos, "Cities")
    
def wordcloud_organizations(DATA_PATH, nb_items, date=None, fig=None, pos=None):
    """ Generate a WordCloud for the organizations for the given date
    
    Arguments:
    nb_items - number of words in the WordCloud

    Returns:
    G - the graph object
    """
    
    if date is not None:
        organizations = pd.read_csv(DATA_PATH + "organizations_final.csv")
        organizations = organizations[organizations.Date == date]
    else:
        organizations = pd.read_csv(DATA_PATH + "organizations_occurences.csv")
    organizations = organizations.set_index("Actor")
    generate_wordcloud(organizations,nb_items, fig, pos, "Organizations")

def plot_occ_graph(DATA_PATH, file, min_actor_rank, edge_size, node_weight_exp, spacing, figsize, community_detection=False):
	""" Generate a co-occurence graph of the top organizations and persons

	Arguments:
	file - file containing the co-occurence data
	min_actor_rank - minimum rank to filter actor on
	edge_size - size of edge of the graph
	node_weight_exp - exponent of the node weight
	spacing - degree of separation between the nodes of the graph
	figsize - size of the matplotlib figure
	community_detection - boolean for community detection
	"""

	df = pd.read_csv(DATA_PATH + '/local_generated/' + file)

	# Read occurence data
	per_occ = pd.read_csv(DATA_PATH + '/from_cluster/wordcloud_actors/persons_occurences.csv', nrows=min_actor_rank)
	org_occ = pd.read_csv(DATA_PATH + '/from_cluster/wordcloud_actors/organizations_occurences.csv', nrows=min_actor_rank)

	# Create a dictionnary that maps an actor to its occurence
	per_occ['Actor'] = per_occ['Actor'].apply(lambda x: x.replace(" ", "_"))
	per_occ['Actor'] = per_occ['Actor'].apply(lambda x: x.replace("-", "_"))
	per_occ_dict = per_occ.set_index('Actor').to_dict()['Count']
	top_persons = list(per_occ_dict.keys())
	org_occ['Actor'] = org_occ['Actor'].apply(lambda x: x.replace(" ", "_"))
	org_occ['Actor'] = org_occ['Actor'].apply(lambda x: x.replace("-", "_"))
	org_occ_dict = org_occ.set_index('Actor').to_dict()['Count']
	top_organizations = list(org_occ_dict.keys())
	actor_occ_dict = dict(per_occ_dict, **org_occ_dict)

	# Create the co-occurence matrix
	docs = df.values.flatten().tolist()
	count_model = CountVectorizer(ngram_range=(1,1)) # default unigram model
	X = count_model.fit_transform(docs)
	Xc = (X.T * X) # this is co-occurrence matrix in sparse csr format
	Xc.setdiag(0)

	# Capitalize and remove underscore in actor names
	nodes = count_model.get_feature_names()
	nodes = [x.replace("_", " ") for x in nodes]
	nodes = [' '.join([x.capitalize() for x in y.split()]) for y in nodes]

	# Create the weighted edge list
	Knz = Xc.nonzero()
	sparserows = Knz[0]
	sparsecols = Knz[1]
	edge_list = [x for x in list(zip(list(sparserows), list(sparsecols))) if x[0] <= x[1]]
	weights = [Xc[x[0], x[1]]*edge_size for x in edge_list]
	weighted_edge_list = [(x[0][0], x[0][1], x[1]) for x in list(zip(edge_list, weights))]
	weighted_edge_list = [(nodes[x[0]], nodes[x[1]], x[2]) for x in weighted_edge_list]

	# Plot the graph
	plt.subplots(figsize=(figsize,figsize))
	G = nx.Graph()
	G.add_weighted_edges_from(weighted_edge_list)
	vertices = [x for x in G.nodes()]
	vertices = [x.replace(" ", "_") for x in vertices]
	vertices = [x.lower() for x in vertices]
	node_sizes = [actor_occ_dict.get(x)**node_weight_exp for x in vertices]
	pos = nx.spring_layout(G, k=spacing/(G.order()**0.5))

	if community_detection:
		partition = community_louvain.best_partition(G, resolution=0.9)
		# add it as an attribute to the nodes
		for n in G.nodes:
		    G.nodes[n]["louvain"] = partition[n]

		nx.draw(G, pos, with_labels=True, font_size = 12, font_weight = 'bold',
		        width=[G[u][v]['weight'] for u,v in G.edges()], node_size=node_sizes, font_color='k', node_color=[G.nodes[n]["louvain"] for n in G.nodes], cmap=plt.cm.jet)
	
	else:
		node_col = ['#ff8d00' if x in top_organizations else '#00c900' for x in vertices]

		nx.draw(G, pos, with_labels=True, font_size = 12, font_weight = 'bold',
	        width=[G[u][v]['weight'] for u,v in G.edges()], node_size=node_sizes, font_color='k', node_color=node_col)

	plt.axis('off')
	plt.show()

	return G

