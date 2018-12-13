import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud

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


def generate_wordcloud(data, size=75):
    """Generate a WordCloud for the items present in data.
    
    Arguments:
    data - a pandas DataFrame with the words as index and a column Count
    size - number of words in the WordCloud
    """
    data = data.sort_values('Count', ascending=False).head(size)
    wordcloud = WordCloud(width=1080, height=920, margin=0).generate_from_frequencies(data.to_dict()['Count'])
    plt.figure(figsize=(15, 10))
    plt.imshow(wordcloud)
    plt.axis('off')
    plt.show() 