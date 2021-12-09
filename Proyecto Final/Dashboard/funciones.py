import streamlit as st
import numpy as np
import pandas as pd
import geopandas as gpd
import branca
import folium
from jinja2 import Template
from streamlit_folium import folium_static
from branca.element import MacroElement
import datetime
from datetime import date
import plotly.express as px
import plotly.graph_objects as go

#from consolidate_data import *
data = pd.read_csv('data_sources/data.csv')

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
    df_global_total_confirmed = df_global_total_confirmed.drop(df_global_total_confirmed.columns[[0, 1, 3, 4, 5, 6, 7]], axis=1)
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
    df_global_death = df_global_death.loc[df_global_death['Status'] == 'Deaths']
    df_global_death.rename(columns={'Country/Region':'name'}, inplace=True)
    df_global_death = df_global_death.drop(df_global_death.columns[[0, 1, 3, 4, 5, 6, 7]], axis=1)
    df_global_death = df_global_death.groupby(by=["name"]).sum()
    df_global_death = df_global_death.fillna(0)

    # Importando datos de casos recuperados
    df_global_recovered = data_filtered.copy()
    df_global_recovered = df_global_recovered.loc[df_global_recovered['Status'] == 'Recovered']
    df_global_recovered.rename(columns={'Country/Region':'name'}, inplace=True)
    df_global_recovered = df_global_recovered.drop(df_global_recovered.columns[[0, 1, 3, 4, 5, 6, 7]], axis=1)
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
    elif status == 'Deaths':
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
    st.title("Introducción")

    st.image('images\covid-19-updates-banner-a.png')

    st.markdown("""

    #### Panorama general

    La enfermedad por coronavirus (COVID-19) es una enfermedad infecciosa causada por el virus SARS-CoV-2. 

    La mayoría de las personas infectadas por el virus experimentarán una enfermedad respiratoria de leve a moderada y se recuperarán sin requerir un tratamiento especial. Sin embargo, algunas enfermarán gravemente y requerirán atención médica. Las personas mayores y las que padecen enfermedades subyacentes, como enfermedades cardiovasculares, diabetes, enfermedades respiratorias crónicas o cáncer, tienen más probabilidades de desarrollar una enfermedad grave. Cualquier persona, de cualquier edad, puede contraer la COVID-19 y enfermar gravemente o morir. 

    La mejor manera de prevenir y ralentizar la transmisión es estar bien informado sobre la enfermedad y cómo se propaga el virus. Protéjase a sí mismo y a los demás de la infección manteniéndose a una distancia mínima de un metro de los demás, llevando una mascarilla bien ajustada y lavándose las manos o limpiándolas con un desinfectante de base alcohólica con frecuencia. Vacúnese cuando le toque y siga las orientaciones locales. 

    El virus puede propagarse desde la boca o nariz de una persona infectada en pequeñas partículas líquidas cuando tose, estornuda, habla, canta o respira. Estas partículas van desde gotículas respiratorias más grandes hasta los aerosoles más pequeños. Es importante adoptar buenas prácticas respiratorias, por ejemplo, tosiendo en la parte interna del codo flexionado, y quedarse en casa y autoaislarse hasta recuperarse si se siente mal.   
    
    """)

    st.write("Fuente: [Organización Mundial de la Salud](https://www.who.int/es/health-topics/coronavirus#tab=tab_1)")

def set_mapa():

    st.title("Distribución geográfica")

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
        status = 'Deaths'
    elif select_status == 'Recuperados':
        status = 'Recovered'
    else:
        status = None

    folium_plot1 = folium_plot(start_date, end_date, status)
    folium_static(folium_plot1)

def set_estadisticas():
    st.title("Estadísticas de incrementos")
    
    # Funcion para agrupar y graficar datos
    def plot_by_date(data, status, color, titulo):
        df = data[data.Status == status]
        df_daily = df.groupby(['Date','Country/Region'], as_index=False).sum()
        df_daily = df_daily[['Date','Country/Region','Cases']]
        df_country = df.groupby(['Country/Region'], as_index=False).sum()

        fig = px.line(df_daily, x="Date", y="Cases", color='Country/Region',
            title=titulo, labels={'Date':'Fecha','Cases':'Casos','Country/Region':'País/Región'})
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
        estado = 'Deaths'
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
    st.title("Otras estadísticas")
    
    selector_pais = st.selectbox(
        label = 'Selecciona un país:',
        options = np.unique(data['Country/Region'])
    )
    st.subheader(f"Casos agrupados por año y mes en {selector_pais}")
    data_summary = data[['Year-month', 'Country/Region', 'Status','Cases']]
    data_summary = data_summary.groupby(['Year-month','Country/Region','Status'], as_index=False).sum()
    data_summary['Year-month'] = data_summary['Year-month'].astype(str)

    period_confirmed = data_summary[data_summary['Status'] == 'Confirmed' ][data_summary['Country/Region'] == selector_pais]
    period_deaths= data_summary[data_summary['Status'] == 'Deaths' ][data_summary['Country/Region'] == selector_pais]
    period_recovered = data_summary[data_summary['Status'] == 'Recovered' ][data_summary['Country/Region'] == selector_pais]

    bar_confirmed  = px.bar(period_confirmed, x='Year-month',y='Cases', color_discrete_sequence=["red"], labels={'Year-month':'Año-mes','Cases':'Casos'}, title='Confirmados por mes')
    bar_deaths  = px.bar(period_deaths, x='Year-month',y='Cases', color_discrete_sequence=["white"], labels={'Year-month':'Año-mes','Cases':'Casos'}, title='Muertes por mes')
    bar_recovered  = px.bar(period_recovered, x='Year-month',y='Cases', color_discrete_sequence=["green"],labels={'Year-month':'Año-mes','Cases':'Casos'}, title='Recuperados por mes' )


    # tasa de mortalidad
    data2 = pd.pivot_table(data,index=['Country/Region','Lat','Long','Date','Year-month'],columns='Status', values='Cases', aggfunc=np.sum, observed=True)
    data2.reset_index(inplace=True)
    data2['Confirmed'] = data2['Confirmed'].fillna(0)
    data2['Deaths'] = data2['Deaths'].fillna(0)
    data2['Recovered'] = data2['Recovered'].fillna(0)
    data2 = data2.groupby(by=['Country/Region','Year-month']).sum()
    data2['Mortality Rate'] = data2['Deaths']/data2['Confirmed']
    data2.reset_index(inplace=True)
    data2 = data2[['Country/Region','Year-month','Mortality Rate','Confirmed','Deaths','Recovered']]
    data2 = data2.dropna(subset=['Mortality Rate'])
    #data2['Mortality Rate'] = data2['Mortality Rate'].astype(float).map(lambda n: '{:.2%}'.format(n))
    data2 = data2[data2['Country/Region']==selector_pais]
    
    total_confirmed = data2['Confirmed'].sum()
    total_deaths = data2['Deaths'].sum()
    total_recovered = data2['Recovered'].sum()

    fig = px.line(data2.round(2), x='Year-month',y='Mortality Rate', labels={'Year-month':'Año-mes','Mortality Rate':'Tasa de mortalidad'}, text='Mortality Rate', markers = True)
    fig.update_traces(
        hovertemplate="<br>".join([
            "Año-mes: %{x}",
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
    

