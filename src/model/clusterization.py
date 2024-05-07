from sklearn.cluster import KMeans
import pandas as pd

class Clusterization():
    def __init__(self,data_componentes:pd.DataFrame)->list:
        self.data_componentes = data_componentes


    def cluster(self):
        data_aux = self.data_componentes
        data_aux =(data_aux - data_aux.min())/(data_aux.max()-data_aux.min())
        data_aux.fillna(0, inplace=True)
        kmeans_model = KMeans(n_clusters=5,random_state=0,init="k-means++")
        y_kmeans=kmeans_model.fit_predict(data_aux)
        kmeans_transform = kmeans_model.transform(data_aux)**2
        data_aux["clusters"] = y_kmeans
        # arr = []
        # for i,cluster in enumerate(kmeans_model.labels_):
        #     arr.append(kmeans_transform[i,cluster])
        
        # data_aux["distancia"] = arr
        componentes = ['WATER','CO2','ENERGY','LOW_EMISSION','EBITDA','GROSS','GLOBAL_NATION']
        data_aux["sum_by_component"] = data_aux[componentes].sum(axis=1)
        df_aux = data_aux.groupby("clusters")
        idx = df_aux["sum_by_component"].transform("max") ==data_aux["sum_by_component"]
        
        return data_aux.index[idx].to_numpy()



        