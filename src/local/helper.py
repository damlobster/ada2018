import pandas as pd

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
    
    
# Join "table" with the list of countries
def join_countries(table, countries, states=None):
    table = table.merge(countries[["Country", "FIPS"]], how="inner", left_on="STATE", right_on="FIPS")
    table.dropna(inplace=True)
    table = table.assign(DATE=pd.to_datetime(table[["YEAR", "MONTH", "DAY"]]))
    #Filter by specific states
    if states is not None:
        table = table[table.STATE.isin(states)]
    #Drop not useful columns
    table.drop(columns=["FIPS", "YEAR", "MONTH", "DAY", "STATE"], inplace=True)
    return table

