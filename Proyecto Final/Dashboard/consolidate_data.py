import pandas as pd
import numpy as np

def data_to_df(path):
    df = pd.read_csv(path, sep = ',')
    df = df.melt(id_vars = ('Province/State','Country/Region','Lat','Long'), var_name='Date', value_name='Cases')
    return df


def consolidate_data(c_path, d_path, r_path):
    'parameters: c_path = path to confirmed cases table, d_path = path to deaths cases table, r_path = path to recovered cases table'
    df1 = data_to_df(c_path)
    df2 = data_to_df(d_path)
    df3 = data_to_df(r_path)

    df1['Status'] = 'Confirmed'
    df2['Status'] = 'Deaths'
    df3['Status'] = 'Recovered'

    data = df1.append(df2)
    data = data.append(df3)
    data['Date'] = pd.to_datetime(data['Date']).dt.date
    data['Year-month'] = pd.to_datetime(data['Date']).dt.to_period('M')

    # lista de paises y status
    paises = np.unique(data['Country/Region'])
    estados = np.unique(data['Status'])
    data_real = pd.DataFrame()

    # ciclo
    for pais in paises:
        for estado in estados:
            # filtrar el dataframe original por pais y estado, remover los índices
            df = data[data['Country/Region'] == pais]
            df = df[df['Status'] == estado]
            df = df.sort_values(by=['Status','Province/State','Date'])
            df.reset_index(inplace=True)

            #calcular un dataframe con lag
            Previous_Cases = df[['Date','Status','Cases']]
            Previous_Cases = Previous_Cases.shift(periods=1)
            Previous_Cases.reset_index(inplace=True)

            #unir ambos dataframe por índice y calcular el valor real de cada día
            df = df.merge(Previous_Cases, left_index=True, right_index=True)
            df['Cases'] =  df['Cases_x'] - df['Cases_y']

            # convertir valores negativos a cero para los días sin datos
            for i in range(len(df)):
                df['Cases'][i] = 0 if df.iloc[i]['Cases'] <0 else df['Cases'][i]
            
            data_real = data_real.append(df)

    #renombrar columnas
    data_real = data_real[['Province/State','Country/Region','Lat','Long','Date_x','Status_x','Year-month','Cases']]
    data_real = data_real.rename(columns={'Date_x':'Date','Status_x':'Status'})

    return data_real

# Define paths for reading data
confirmed_path = 'data_sources/time_series_covid19_confirmed_global.csv'
deaths_path = 'data_sources/time_series_covid19_deaths_global.csv'
recovered_path = 'data_sources/time_series_covid19_recovered_global.csv'

data = consolidate_data(confirmed_path, deaths_path, recovered_path)