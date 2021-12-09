import streamlit as st
import numpy as np
import pandas as pd
from funciones import *


st.set_page_config(page_title='Covid-19 Dashboard',
                   page_icon='😷',
                   layout="wide",
                   menu_items={'About': "Este tablero fue desarrollado como un proyecto end-to-end para el curso de Product Development de Universidad Galielo (2021)."})


#Sidebar
st.sidebar.header('COVID-19')

menu = st.sidebar.radio(
    "",
    ("Inicio", "Distribución geográfica", "Estadísticas de incrementos", "Otras estadísticas","Acerca de este proyecto"),
)

st.sidebar.markdown('---')
st.sidebar.write("""
    Desarrollado por: 
    * Alejandro López 
    * Eddson Sierra 
    * Diego Alvarez 
    * Jairo Salazar 
    * Sergio Palma""")

st.sidebar.markdown('---')
st.sidebar.markdown("""
    ###### Proyecto final\n 
    ###### Product Development\n
    ###### Universidad Galileo (2021)
""")
st.sidebar.image('images/galileo.png', width=100)

if menu == 'Inicio':
    set_inicio()
elif menu == 'Distribución geográfica':
    set_mapa()
elif menu == 'Estadísticas de incrementos':
    set_estadisticas()
elif menu == "Otras estadísticas":
    set_otras_estadisticas()
elif menu == "Acerca de este proyecto":
    set_acerca_de()