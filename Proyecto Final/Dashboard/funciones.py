import streamlit as st
import numpy as np
import pandas as pd
import geopandas as gpd
import branca
import folium
import requests
from jinja2 import Template
from streamlit_folium import folium_static
from branca.element import MacroElement
import datetime
from datetime import date
import plotly.express as px
import plotly.graph_objects as go

#from consolidate_data import *
#data = pd.read_csv('data_sources/data.csv')

base_url = 'http://172.16.1.33:80/'

def get_data(url, endpoint):
    r = requests.get(url+endpoint)
    json = r.json()
    df = pd.DataFrame(json['data'])
    return df

data_confirmed = get_data(base_url, 'confirmed')
data_death = get_data(base_url, 'death')
data_recovered = get_data(base_url, 'recovered')

frames = [data_confirmed, data_death, data_recovered]

data = pd.concat(frames)

data = data.drop(['CasesOrig'], axis=1)
data['Date'] = pd.to_datetime(data['Date']).dt.date
data['Year-month'] = pd.to_datetime(data['Year-month']).dt.to_period('M')

# Clase para vincula un mapa de colores a una capa determinada
class BindColormap(MacroElement):
    """ Vincula un mapa de colores a una capa determinada.
    
    Parametros
    ----------
    colormap : branca.colormap.ColorMap
        Mapa de colores a vincular.
    """
    def __init__(self, layer, colormap):
        super(BindColormap, self).__init__()
        self.layer = layer
        self.colormap = colormap
        self._template = Template(u"""
        {% macro script(this, kwargs) %}
            {{this.colormap.get_name()}}.svg[0][0].style.display = 'block';
            {{this._parent.get_name()}}.on('overlayadd', function (eventLayer) {
                if (eventLayer.layer == {{this.layer.get_name()}}) {
                    {{this.colormap.get_name()}}.svg[0][0].style.display = 'block';
                }});
            {{this._parent.get_name()}}.on('overlayremove', function (eventLayer) {
                if (eventLayer.layer == {{this.layer.get_name()}}) {
                    {{this.colormap.get_name()}}.svg[0][0].style.display = 'none';
                }});
        {% endmacro %}
        """)


# Funcion para generar el mapa empleando folium 
def folium_plot(start_date, end_date, status):
    # Definiendo el path del archivo JSON con la geometria de los paises
    country_shapes = 'map_sources\world-countries.json'
    data_filtered = data.copy()
    data_filtered['Date'] = pd.to_datetime(data_filtered['Date'], errors='coerce')
    data_filtered = data_filtered[(data_filtered['Date']>=start_date) & (data_filtered['Date']<=end_date)]

    # Importando datos de casos confirmados
    df_global_total_confirmed = data_filtered.copy()
    df_global_total_confirmed = df_global_total_confirmed.loc[df_global_total_confirmed['Status'] == 'Confirmed']
    df_global_total_confirmed.rename(columns={'Country/Region':'name'}, inplace=True)
    df_global_total_confirmed = df_global_total_confirmed.drop(['Province/State', 'Lat', 'Long', 'Date', 'Status', 'Year-month'], axis=1)
    #df_global_total_confirmed = df_global_total_confirmed.drop(df_global_total_confirmed.columns[[0, 1, 3, 4, 5, 6, 7]], axis=1)
    df_global_total_confirmed = df_global_total_confirmed.groupby(by=["name"]).sum()

    # Importando datos con la geometria de los paises
    geoJSON_df = gpd.read_file(country_shapes)

    # Renombrando datos del archivo de geometria
    geoJSON_df["name"].replace({'United States of America':'US'}, inplace = True)
    geoJSON_df["name"].replace({'South Korea':'Korea, South'}, inplace = True)
    geoJSON_df["name"].replace({'The Bahamas':'Bahamas'}, inplace = True)
    geoJSON_df["name"].replace({'Ivory Coast':'Cote d\'Ivoire'}, inplace = True)
    geoJSON_df["name"].replace({'Republic of the Congo':'Congo (Brazzaville)'}, inplace = True)
    geoJSON_df["name"].replace({'Democratic Republic of the Congo':'Congo (Kinshasa)'}, inplace = True)
    geoJSON_df["name"].replace({'United Republic of Tanzania':'Tanzania'}, inplace = True)
    geoJSON_df["name"].replace({'Czech Republic':'Czechia'}, inplace = True)
    geoJSON_df["name"].replace({'Republic of Serbia':'Serbia'}, inplace = True)

    final_total_cases = geoJSON_df.merge(df_global_total_confirmed,how="left",on = "name")
    final_total_cases = final_total_cases.fillna(0)

    # Importando datos de muertes
    df_global_death = data_filtered.copy()
    df_global_death = df_global_death.loc[df_global_death['Status'] == 'Death']
    df_global_death.rename(columns={'Country/Region':'name'}, inplace=True)
    df_global_death = df_global_death.drop(['Province/State', 'Lat', 'Long', 'Date', 'Status', 'Year-month'], axis=1)
    df_global_death = df_global_death.groupby(by=["name"]).sum()
    df_global_death = df_global_death.fillna(0)

    # Importando datos de casos recuperados
    df_global_recovered = data_filtered.copy()
    df_global_recovered = df_global_recovered.loc[df_global_recovered['Status'] == 'Recovered']
    df_global_recovered.rename(columns={'Country/Region':'name'}, inplace=True)
    df_global_recovered = df_global_recovered.drop(['Province/State', 'Lat', 'Long', 'Date', 'Status', 'Year-month'], axis=1)
    df_global_recovered = df_global_recovered.groupby(by=["name"]).sum()
    df_global_recovered = df_global_recovered.fillna(0)

    # Preparando datos de casos confirmados para ser concatenados    
    df_global_folium = final_total_cases.copy()
    df_global_folium = df_global_folium.iloc[:,[1,2,-1]]
    df_global_folium.rename(columns={ df_global_folium.columns[-1]: "confirmados" }, inplace = True)

    # Preparando datos de muertes para ser concatenados
    df_global_death.reset_index(level=0,inplace=True)
    df_global_death_name_last_column = df_global_death.iloc[:,[0,-1]]
    df_global_death_name_last_column.rename(columns={ df_global_death_name_last_column.columns[-1]: "muertes" }, inplace = True)

    # Preparando datos de casos recuperados para ser concatenados
    df_global_recovered.reset_index(level=0,inplace=True)
    df_global_recovered_name_last_column = df_global_recovered.iloc[:,[0,-1]]
    df_global_recovered_name_last_column.rename(columns={ df_global_recovered_name_last_column.columns[-1]: "recuperados" }, inplace = True)

    # Concatenando datos
    df_global_folium = df_global_folium.merge(df_global_death_name_last_column,how="left", on = "name")
    df_global_folium = df_global_folium.merge(df_global_recovered_name_last_column,how="left", on = "name")


    # Definicion de mapa de colores incluyendo minimo y maximo para casos confirmados
    cmap1 = branca.colormap.StepColormap(
        colors=['#fff600','#ffc302','#ff5b00','#ff0505'],
        vmin=0,
        vmax=df_global_folium['confirmados'].max(),  
        caption='Casos Confirmados')

    # Definicion de mapa de colores incluyendo minimo y maximo para muertes
    cmap2 = branca.colormap.StepColormap(
        colors=["#fef0d9",'#fdcc8a','#fc8d59','#d7301f'],
        vmin=0,
        vmax=df_global_folium['muertes'].max(),  
        caption='Muertes')

    # Definicion de mapa de colores incluyendo minimo y maximo para casos recuperados
    cmap3 = branca.colormap.StepColormap(
        colors=["#edf8fb",'#b3cde3','#8856a7','#810f7c'],
        vmin=0,
        vmax=df_global_folium['recuperados'].max(),  
        caption='Recuperados')
    
    # Definicion de colores para visualizacion dependiendo del filtro aplicado
    if status == 'Confirmed':
        cmaps = [cmap1]
        columns_list_global_map = ["confirmados"]
        colors = ["YlOrRd"]
    elif status == 'Death':
        cmaps = [cmap2]
        columns_list_global_map = ["muertes"]
        colors = ["OrRd"]
    elif status == 'Recovered':
        cmaps = [cmap3]
        columns_list_global_map = ["recuperados"]
        colors = ["BuPu"]
    else:
        cmaps = [cmap1, cmap2, cmap3]
        columns_list_global_map = ["confirmados", "muertes", "recuperados"]
        colors = ["YlOrRd","OrRd","BuPu"]

    # Creando folium map
    folium_map_covid = folium.Map(location=[35,0], zoom_start=1)

    # Ciclo para agregar parametros y colores
    for color, cmap, i in zip(colors, cmaps, columns_list_global_map):
        choropleth = folium.Choropleth(
        geo_data=df_global_folium,
        data=df_global_folium,
        name=i,
        columns=['name',i],
        key_on="feature.properties.name",
        fill_color=color,
        colormap= cmap,
        fill_opacity=1,
        line_opacity=0.2,
        show=True
        )
        
        for child in choropleth._children:
            if child.startswith("color_map"):
                del choropleth._children[child]

        style_function1 = lambda x: {'fillColor': '#ffffff', 
                            'color':'#000000', 
                            'fillOpacity': 0.1, 
                            'weight': 0.1}
        
        highlight_function1 = lambda x: {'fillColor': '#000000', 
                                'color':'#000000', 
                                'fillOpacity': 0.50, 
                                'weight': 0.1}
        
        NIL1 = folium.features.GeoJson(
            data = df_global_folium,
            style_function=style_function1, 
            control=False,
            highlight_function=highlight_function1, 
            tooltip=folium.features.GeoJsonTooltip(
                fields=['name',"confirmados", "muertes", "recuperados"],
                aliases=['name',"confirmados", "muertes", "recuperados"],
                style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"),
                localize=True 
            )
        )
        folium_map_covid.add_child(NIL1)
        folium_map_covid.keep_in_front(NIL1)

        folium_map_covid.add_child(cmap)
                
        folium_map_covid.add_child(choropleth)
        
        bc = BindColormap(choropleth, cmap)
        
        folium_map_covid.add_child(bc)
    
    folium.TileLayer('cartodbdark_matter',name="dark mode",control=True).add_to(folium_map_covid)
    folium.TileLayer('cartodbpositron',name="light mode",control=True).add_to(folium_map_covid)

    folium_map_covid.add_child(folium.LayerControl())
    return folium_map_covid

def set_inicio():
    st.title("Introducci??n")

    st.image('images\covid-19-updates-banner-a.png')

    st.markdown("""

    #### Panorama general

    La enfermedad por coronavirus (COVID-19) es una enfermedad infecciosa causada por el virus SARS-CoV-2. 

    La mayor??a de las personas infectadas por el virus experimentar??n una enfermedad respiratoria de leve a moderada y se recuperar??n sin requerir un tratamiento especial. Sin embargo, algunas enfermar??n gravemente y requerir??n atenci??n m??dica. Las personas mayores y las que padecen enfermedades subyacentes, como enfermedades cardiovasculares, diabetes, enfermedades respiratorias cr??nicas o c??ncer, tienen m??s probabilidades de desarrollar una enfermedad grave. Cualquier persona, de cualquier edad, puede contraer la COVID-19 y enfermar gravemente o morir. 

    La mejor manera de prevenir y ralentizar la transmisi??n es estar bien informado sobre la enfermedad y c??mo se propaga el virus. Prot??jase a s?? mismo y a los dem??s de la infecci??n manteni??ndose a una distancia m??nima de un metro de los dem??s, llevando una mascarilla bien ajustada y lav??ndose las manos o limpi??ndolas con un desinfectante de base alcoh??lica con frecuencia. Vac??nese cuando le toque y siga las orientaciones locales. 

    El virus puede propagarse desde la boca o nariz de una persona infectada en peque??as part??culas l??quidas cuando tose, estornuda, habla, canta o respira. Estas part??culas van desde got??culas respiratorias m??s grandes hasta los aerosoles m??s peque??os. Es importante adoptar buenas pr??cticas respiratorias, por ejemplo, tosiendo en la parte interna del codo flexionado, y quedarse en casa y autoaislarse hasta recuperarse si se siente mal.   
    
    """)

    st.write("Fuente: [Organizaci??n Mundial de la Salud](https://www.who.int/es/health-topics/coronavirus#tab=tab_1)")

def set_mapa():

    st.title("Distribuci??n geogr??fica")

    selector_date = st.selectbox(
        label = 'Seleccione el tipo de ingreso de la fecha:',
        options = ['Calendario', 'Slider']
    )

    if 'date_bounds' not in st.session_state:
        date_min = datetime.datetime.strptime(str(min(data['Date'])), '%Y-%m-%d')
        date_max = datetime.datetime.strptime(str(max(data['Date'])), '%Y-%m-%d')
        st.session_state.date_bounds = (date_min, date_max)
    
    if selector_date == 'Slider':
        date_container_1 = st.empty()
        start_date, end_date = date_container_1.slider('Rango de fechas a visualizar: ', value=st.session_state.date_bounds, format='DD-MM-YYYY')
    elif selector_date == 'Calendario':
        date_container_1 = st.empty()
        dates = date_container_1.date_input('Rango de fechas a visualizar: ', value=st.session_state.date_bounds, min_value=st.session_state.date_bounds[0], max_value=st.session_state.date_bounds[1])
        if len(dates) == 2:
            start_date = dates[0].strftime('%Y-%m-%d')
            end_date = dates[1].strftime('%Y-%m-%d')
        else:
            start_date = dates[0].strftime('%Y-%m-%d')
            end_date = st.session_state.date_bounds[1].strftime('%Y-%m-%d')

    select_status = st.radio(
        label='Seleccionar un estado:',
        options= ['Confirmados','Muertes','Recuperados', 'Todos los datos'],
        index=0
    )

    if select_status == 'Confirmados':
        status = 'Confirmed'
    elif select_status == 'Muertes':
        status = 'Death'
    elif select_status == 'Recuperados':
        status = 'Recovered'
    else:
        status = None

    folium_plot1 = folium_plot(start_date, end_date, status)
    folium_static(folium_plot1)

def set_estadisticas():
    st.title("Estad??sticas de incrementos")
    
    # Funcion para agrupar y graficar datos
    def plot_by_date(data, status, color, titulo):
        df = data[data.Status == status]
        df_daily = df.groupby(['Date','Country/Region'], as_index=False).sum()
        df_daily = df_daily[['Date','Country/Region','Cases']]
        df_country = df.groupby(['Country/Region'], as_index=False).sum()

        fig = px.line(df_daily, x="Date", y="Cases", color='Country/Region',
            title=titulo, labels={'Date':'Fecha','Cases':'Casos','Country/Region':'Pa??s/Regi??n'})
        fig.update_traces(patch={"line": {"width": 2}})
        
        barras = px.bar(df_country, x='Country/Region',y='Cases', color='Cases')
        barras.update_layout( xaxis={'categoryorder':'total descending'})
        return (df_daily, fig, barras)

    # Filtro de estado:
    select_status = st.radio(
        label='Seleccionar un estado:',
        options= ['Confirmados','Muertes','Recuperados'],
        index=0
    )

    # Radio Selection estado
    if select_status == 'Confirmados':
        estado = 'Confirmed'
    elif select_status == 'Muertes':
        estado = 'Death'
    else:
        estado = 'Recovered'
    
    df, fig, barras = plot_by_date(data,estado,["#FFA500"], f"{select_status} por fecha")

        
    #columnas
    col1, col2 = st.columns([2,1])

    with col1:
        st.plotly_chart(fig, use_container_width = True)

    with col2:
        st.plotly_chart(barras,use_container_width = True)




def set_otras_estadisticas():
    st.title("Otras estad??sticas")
    
    selector_pais = st.selectbox(
        label = 'Selecciona un pa??s:',
        options = np.unique(data['Country/Region'])
    )
    st.subheader(f"Casos agrupados por a??o y mes en {selector_pais}")
    data_summary = data[['Year-month', 'Country/Region', 'Status','Cases']]
    data_summary = data_summary.groupby(['Year-month','Country/Region','Status'], as_index=False).sum()
    data_summary['Year-month'] = data_summary['Year-month'].astype(str)

    period_confirmed = data_summary[data_summary['Status'] == 'Confirmed' ][data_summary['Country/Region'] == selector_pais]
    period_deaths= data_summary[data_summary['Status'] == 'Death' ][data_summary['Country/Region'] == selector_pais]
    period_recovered = data_summary[data_summary['Status'] == 'Recovered' ][data_summary['Country/Region'] == selector_pais]

    bar_confirmed  = px.bar(period_confirmed, x='Year-month',y='Cases', color_discrete_sequence=["red"], labels={'Year-month':'A??o-mes','Cases':'Casos'}, title='Confirmados por mes')
    bar_deaths  = px.bar(period_deaths, x='Year-month',y='Cases', color_discrete_sequence=["white"], labels={'Year-month':'A??o-mes','Cases':'Casos'}, title='Muertes por mes')
    bar_recovered  = px.bar(period_recovered, x='Year-month',y='Cases', color_discrete_sequence=["green"],labels={'Year-month':'A??o-mes','Cases':'Casos'}, title='Recuperados por mes' )


    # tasa de mortalidad
    data2 = pd.pivot_table(data,index=['Country/Region','Lat','Long','Date','Year-month'],columns='Status', values='Cases', aggfunc=np.sum, observed=True)
    data2.reset_index(inplace=True)
    data2['Confirmed'] = data2['Confirmed'].fillna(0)
    data2['Death'] = data2['Death'].fillna(0)
    data2['Recovered'] = data2['Recovered'].fillna(0)
    data2 = data2.groupby(by=['Country/Region','Year-month']).sum()
    data2['Mortality Rate'] = data2['Death']/data2['Confirmed']*100
    data2.reset_index(inplace=True)
    data2 = data2[['Country/Region','Year-month','Mortality Rate','Confirmed','Death','Recovered']]
    data2 = data2.dropna(subset=['Mortality Rate'])
    data2 = data2[data2['Country/Region']==selector_pais]
    total_confirmed = data2['Confirmed'].sum()
    total_deaths = data2['Death'].sum()
    total_recovered = data2['Recovered'].sum()
    data2['Year-month'] = data2['Year-month'].dt.to_timestamp('s').dt.strftime('%Y-%m')

    fig = px.line(data2.round(2), x='Year-month',y='Mortality Rate', labels={'Year-month':'A??o-mes','Mortality Rate':'Tasa de mortalidad'}, text='Mortality Rate', markers = True)
    fig.update_traces(
        hovertemplate="<br>".join([
            "A??o-mes: %{x}",
            "Tasa de mortalidad: %{y} %",
            ])
    )

    #layout
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric('Total confirmados',value='{:,.0f}'.format(total_confirmed))
        st.plotly_chart(bar_confirmed, use_container_width = True)

    with col2:
        st.metric('Total muertes',value='{:,.0f}'.format(total_deaths))
        st.plotly_chart(bar_deaths,use_container_width = True)

    with col3:
        st.metric('Total recuperados',value='{:,.0f}'.format(total_recovered))
        st.plotly_chart(bar_recovered, use_container_width = True)

    st.subheader(f'Tasa de mortalidad % (Muertes/Casos confirmados)')
    st.plotly_chart(fig, use_container_width = True)


def set_acerca_de():
    st.header("Proyecto final")
    st.markdown(
        body="""
        El presente dashboard, front-end y back-end fueron desarrollados como parte del proyecto final end-to-end del curso de Product Development de Universidad Galileo (2021) en la Maestr??a de Ciencia de Datos. El c??digo est?? desplegado mediante un contenedor de Docker,  la ingesta de datos se realiz?? en Airflow, la interfaz de comunicaci??n se realiz?? a trav??s de FastAPI y el tablero con streamlit. 
        
        El c??digo est?? disponible en: https://github.com/eddson90/product_development/tree/master/Proyecto%20Final
        
        Todos los derechos reservados.
        
        """

    )
    

