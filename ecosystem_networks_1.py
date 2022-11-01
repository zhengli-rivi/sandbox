import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network

df = pd.read_csv('ecosystem_networks.csv', header = 0)
df.describe()
df.shape
df_2K_sample = df.sample(n=2000,replace = False)
df_2K_sample.shape

G = nx.from_pandas_edgelist(df_2K_sample, source = 'client_name', target = 'company_name', edge_attr = 'Weight') 
# need to tune weight parameters, maybe based on the job status (placed > )

print("No of unique characters:", len(G.nodes))

print("No of connections:", len(G.edges))

# import pyvis
from pyvis.network import Network
# create vis network
net = Network(notebook=False, width=1500, height=1000, select_menu=True, filter_menu=True)
# load the networkx graph
net.from_nx(G)
# show
net.show("2k_sample_networks.html")