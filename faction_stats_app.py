import requests
import pandas as pd
import ast
import concurrent.futures
import streamlit as st
import plotly
import plotly.express as px
from PIL import Image
import time


dust = pd.read_csv('http://165.22.125.123/meteor_dust_nfts.csv')
dust['spirit_level'] = dust['traits'].apply(lambda x: ast.literal_eval(x).get('Spirit Level'))
#dust[['user_addr', 'spirit_level']]
nested = pd.read_csv('http://165.22.125.123/nested_egg_nfts.csv')
nested['spirit_level'] = nested['traits'].apply(lambda x: ast.literal_eval(x).get('Spirit Level'))
#nested[['user_addr', 'spirit_level']]

spirit_df = pd.concat([dust[['user_addr', 'spirit_level']], nested[['user_addr', 'spirit_level']]]).reset_index(drop=True)
spirit_df['spirit_level'] = spirit_df['spirit_level'].astype('float')
spirit_df = spirit_df[spirit_df['spirit_level'] > 0]
spirit_df = spirit_df.groupby(['user_addr']).sum().reset_index()

out = []
CONNECTIONS = 100
TIMEOUT = 5


def load_url(address, timeout):
    ans = requests.get('https://stations.levana.finance/api/factions?wallet=' + address, timeout=timeout)
    return [ans.json().get('wallet').get('faction'), address] #+ (' - ') + url.remove('https://stations.levana.finance/api/factions?wallet=')
    
with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, address, TIMEOUT) for address in spirit_df['user_addr'])
    time1 = time.time()

    for future in concurrent.futures.as_completed(future_to_url):
        try:
            data = future.result()
        except Exception as exc:
            data = str(type(exc))
        finally:
            out.append(data)

            print(str(len(out)),end="\r")

    time2 = time.time()

#print(f'Took {time2-time1:.2f} s')


address_faction = pd.DataFrame(list(filter(lambda x: x != "<class 'AttributeError'>", out)))
address_faction.rename(columns={0:'faction', 1:'user_addr'}, inplace=True)

spirit_df = spirit_df.merge(address_faction, how='inner', on='user_addr')
spirit_per_faction = spirit_df.groupby(['faction']).sum().reset_index().rename(columns={0:'faction'})

st.set_page_config(layout="wide")
levana = Image.open("faction_wars.png")
st.image(levana)

st.markdown("""## Levana Faction Wars Tracker """)
st.text('Currently tracking spirit levels')

st.dataframe(spirit_df)

st.download_button(
    "Press to Download",
    spirit_df.to_csv().encode('utf-8'),
    "spirit_level_per_staked_address.csv",
    "text/csv",
    key='download-csv'
    )

st.dataframe(spirit_per_faction)