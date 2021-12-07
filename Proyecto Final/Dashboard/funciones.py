import streamlit as st
import numpy as np
import pandas as pd
import datetime
from datetime import date

import plotly.express as px

#from consolidate_data import *
data = pd.read_csv('data_sources/data.csv')

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

def set_estadisticas():
    st.title("Estadísticas de incrementos")
    
    # Funcion para agrupar y graficar datos
    def plot_by_date(data, status, color, titulo):
        df = data[data.Status == status]
        df_daily = df.groupby(['Date','Country/Region'], as_index=False).sum()
        df_daily = df_daily[['Date','Country/Region','Cases']]
        df_country = df.groupby(['Country/Region'], as_index=False).sum()

        fig = px.line(df_daily, x="Date", y="Cases", color='Country/Region',
            title=titulo)
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

    data_summary = data[['Year-month', 'Country/Region', 'Status','Cases']]
    data_summary = data_summary.groupby(['Year-month','Country/Region','Status'], as_index=False).sum()
    data_summary['Year-month'] = data_summary['Year-month'].astype(str)

    period_confirmed = data_summary[data_summary['Status'] == 'Confirmed' ][data_summary['Country/Region'] == selector_pais]
    period_deaths= data_summary[data_summary['Status'] == 'Deaths' ][data_summary['Country/Region'] == selector_pais]
    period_recovered = data_summary[data_summary['Status'] == 'Recovered' ][data_summary['Country/Region'] == selector_pais]

    bar_confirmed  = px.bar(period_confirmed, x='Year-month',y='Cases', color_discrete_sequence=["red"], labels={'Year-month':'Año-mes','Cases':'Casos'})
    bar_deaths  = px.bar(period_deaths, x='Year-month',y='Cases', color_discrete_sequence=["white"], labels={'Year-month':'Año-mes','Cases':'Casos'})
    bar_recovered  = px.bar(period_recovered, x='Year-month',y='Cases', color_discrete_sequence=["green"],labels={'Year-month':'Año-mes','Cases':'Casos'} )
    
    col1, col2, col3 = st.columns(3)

    with col1:
        st.write("Confirmados por mes")
        st.plotly_chart(bar_confirmed, use_container_width = True)

    with col2:
        st.write("Muertes por mes")
        st.plotly_chart(bar_deaths,use_container_width = True)

    with col3:
        st.write("Recuperados por mes")
        st.plotly_chart(bar_recovered, use_container_width = True)

