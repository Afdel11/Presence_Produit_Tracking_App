import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Présence Produits",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Fonction pour charger le logo
def load_logo():
    """Charge et affiche le logo de l'entreprise"""
    try:
        # Essayer de charger le logo depuis un fichier local
        return st.image("image.png", width=200)
    except:
        # Si le logo n'est pas trouvé, afficher un placeholder
        return st.markdown("**🏢 LOGO ENTREPRISE**")

# CSS personnalisé pour un design moderne
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 30px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    
    .header-content {
        flex: 1;
        text-align: center;
    }
    
    .logo-container {
        flex: 0 0 auto;
        margin-right: 20px;
    }
    
    .kpi-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
        margin-bottom: 20px;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    .stSelectbox > div > div > div {
        background-color: #f8f9fa;
        border-radius: 5px;
    }
    
    .chart-container {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Fonction de chargement des données avec cache
@st.cache_data(ttl=3600)  # Cache pendant 1 heure
def load_data():
    """Charge et fusionne les données depuis la base"""
    try:
        engine = create_engine("postgresql://datanalyst:AVNS_iRGWt6_LRkZk6w7ggUh@pg-2b8f58a-maad-733a.i.aivencloud.com:20738/defaultdb?sslmode=require")
        
        # Charger les données
        tracking = pd.read_sql("SELECT * FROM tracking_presence", engine)
        produits = pd.read_sql("SELECT * FROM produits", engine)
        points_de_vente = pd.read_sql("SELECT * FROM points_de_vente", engine)
        
        # Fusionner les données
        df = tracking.merge(produits, how='left', left_on='product_id', right_on='id', suffixes=('', '_produit'))
        df.drop(columns=['id'], inplace=True)
        df.rename(columns={'nom': 'nom_produit'}, inplace=True)
        
        # Préparer la fusion avec points de vente
        df['id_point_de_vente'] = df['id_point_de_vente'].astype(str)
        points_de_vente['nom'] = points_de_vente['nom'].astype(str)
        
        # Fusion avec points de vente
        df = df.merge(points_de_vente, how='left', left_on='id_point_de_vente', right_on='nom', suffixes=('', '_point_vente'))
        df.drop(columns=['id'], inplace=True)
        df.rename(columns={'nom': 'nom_point_vente'}, inplace=True)
        
        # Nettoyer et enrichir les données
        df = df.loc[:, ~df.columns.duplicated()]
        df['created_on'] = pd.to_datetime(df['created_on'])
        df['date_ouverture'] = pd.to_datetime(df['date_ouverture'], errors='coerce')
        df['date_creation'] = pd.to_datetime(df['date_creation'], errors='coerce')
        
        # Variables temporelles - utiliser date_ouverture si disponible, sinon created_on
        df['date_reference'] = df['date_ouverture'].fillna(df['created_on'])
        df['annee'] = df['date_reference'].dt.year
        df['mois'] = df['date_reference'].dt.month
        df['jour_semaine'] = df['date_reference'].dt.day_name()
        df['semaine'] = df['date_reference'].dt.isocalendar().week
        df['date'] = df['date_reference'].dt.date
        
        # Supprimer les colonnes en double et inutiles
        colonnes_a_supprimer = []
        
        # Supprimer date_creation car on a déjà les nouvelles variables temporelles
        if 'date_creation' in df.columns:
            colonnes_a_supprimer.append('date_creation')
        
        # Supprimer les colonnes dupliquées ou redondantes
        # Si on a id_point_de_vente et nom_point_vente, on garde nom_point_vente (plus lisible)
        if 'id_point_de_vente' in df.columns and 'nom_point_vente' in df.columns:
            colonnes_a_supprimer.append('id_point_de_vente')
        
        # Supprimer les colonnes identifiées
        df.drop(columns=[col for col in colonnes_a_supprimer if col in df.columns], inplace=True)
        
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {str(e)}")
        return None

# Fonction pour calculer les KPIs
def calculate_kpis(df):
    """Calcule les KPIs principaux"""
    total_observations = len(df)
    total_presences = df['value'].sum()
    taux_presence_global = (total_presences / total_observations) * 100 if total_observations > 0 else 0
    
    nb_produits = df['product_id'].nunique()
    nb_points_vente = df['nom_point_vente'].nunique()
    nb_marques = df['marque'].nunique()
    nb_segments = df['segment'].nunique()
    nb_zones = df['zone'].nunique()
    
    return {
        'total_observations': total_observations,
        'total_presences': total_presences,
        'taux_presence_global': taux_presence_global,
        'nb_produits': nb_produits,
        'nb_points_vente': nb_points_vente,
        'nb_marques': nb_marques,
        'nb_segments': nb_segments,
        'nb_zones': nb_zones
    }

# Fonction pour afficher les KPIs
def display_kpis(kpis):
    """Affiche les KPIs dans des cartes colorées"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📊 Observations</h3>
            <h2>{kpis['total_observations']:,}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>✅ Présences</h3>
            <h2>{kpis['total_presences']:,}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📈 Taux Global</h3>
            <h2>{kpis['taux_presence_global']:.1f}%</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🛍️ Produits</h3>
            <h2>{kpis['nb_produits']}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    # Ligne supplémentaire de KPIs
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric("🏪 Points de Vente", kpis['nb_points_vente'])
    with col6:
        st.metric("🏷️ Marques", kpis['nb_marques'])
    with col7:
        st.metric("📋 Segments", kpis['nb_segments'])
    with col8:
        st.metric("🌍 Zones", kpis['nb_zones'])

# Fonction pour créer le graphique en barres par marque
def create_brand_chart(df_filtered):
    """Crée le graphique de taux de présence par marque"""
    brand_stats = df_filtered.groupby('marque').agg({
        'value': ['count', 'sum', 'mean'],
        'product_id': 'nunique'
    }).round(3)
    
    brand_stats.columns = ['Observations', 'Presences', 'Taux_Presence', 'Nb_Produits']
    brand_stats = brand_stats.sort_values('Taux_Presence', ascending=False).head(15)
    
    fig = px.bar(
        brand_stats.reset_index(),
        x='marque',
        y='Taux_Presence',
        color='Taux_Presence',
        color_continuous_scale='Viridis',
        title="📊 Taux de Présence par Marque (Top 15)",
        labels={'Taux_Presence': 'Taux de Présence', 'marque': 'Marque'}
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        showlegend=False
    )
    
    return fig

# Fonction pour créer le graphique par segment
def create_segment_chart(df_filtered):
    """Crée le graphique de performance par segment"""
    segment_stats = df_filtered.groupby('segment').agg({
        'value': ['count', 'sum', 'mean']
    }).round(3)
    
    segment_stats.columns = ['Observations', 'Presences', 'Taux_Presence']
    segment_stats = segment_stats.sort_values('Taux_Presence', ascending=True)
    
    fig = px.bar(
        segment_stats.reset_index(),
        x='Taux_Presence',
        y='segment',
        orientation='h',
        color='Taux_Presence',
        color_continuous_scale='RdYlBu',
        title="📋 Performance par Segment",
        labels={'Taux_Presence': 'Taux de Présence', 'segment': 'Segment'}
    )
    
    fig.update_layout(height=600)
    
    return fig

# Fonction pour créer la carte géographique
def create_geo_chart(df_filtered):
    """Crée la carte de géolocalisation"""
    df_geo = df_filtered.dropna(subset=['latitude', 'longitude'])
    
    if len(df_geo) > 0:
        zone_stats = df_geo.groupby(['zone', 'latitude', 'longitude']).agg({
            'value': ['count', 'sum', 'mean']
        }).round(3)
        
        zone_stats.columns = ['Observations', 'Presences', 'Taux_Presence']
        zone_stats = zone_stats.reset_index()
        
        fig = px.scatter_mapbox(
            zone_stats,
            lat='latitude',
            lon='longitude',
            color='Taux_Presence',
            size='Observations',
            hover_name='zone',
            hover_data=['Observations', 'Presences'],
            color_continuous_scale='RdYlGn',
            title="🗺️ Répartition Géographique des Taux de Présence",
            mapbox_style='open-street-map',
            zoom=6
        )
        
        fig.update_layout(height=600)
        
        return fig
    else:
        return None

# Fonction pour créer le graphique temporel
def create_time_chart(df_filtered):
    """Crée le graphique d'évolution temporelle basé sur date_reference"""
    # Utiliser date_reference pour l'analyse temporelle
    daily_stats = df_filtered.groupby('date').agg({
        'value': ['count', 'sum', 'mean']
    }).round(3)
    
    daily_stats.columns = ['Observations', 'Presences', 'Taux_Presence']
    daily_stats = daily_stats.reset_index()
    
    # Créer un graphique avec des informations sur la source des dates
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(
            'Évolution des Observations (basée sur date d\'ouverture des points de vente)', 
            'Évolution du Taux de Présence (basée sur date d\'ouverture des points de vente)'
        ),
        vertical_spacing=0.1
    )
    
    # Graphique des observations
    fig.add_trace(
        go.Scatter(
            x=daily_stats['date'],
            y=daily_stats['Observations'],
            mode='lines+markers',
            name='Observations',
            line=dict(color='#636EFA', width=2)
        ),
        row=1, col=1
    )
    
    # Graphique du taux de présence
    fig.add_trace(
        go.Scatter(
            x=daily_stats['date'],
            y=daily_stats['Taux_Presence'],
            mode='lines+markers',
            name='Taux de Présence',
            line=dict(color='#EF553B', width=2)
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        title="📅 Évolution Temporelle (Date d'ouverture des points de vente)",
        height=600,
        showlegend=True
    )
    
    return fig

# Interface principale
def main():
    # Header avec logo
    col_logo, col_title = st.columns([1, 4])
    
    with col_logo:
        load_logo()
    
    with col_title:
        st.markdown("""
        <div class="main-header">
            <div class="header-content">
                <h1>📊 Dashboard Présence Produits</h1>
                <p>Analyse et visualisation de la présence produits dans le réseau de distribution</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Chargement des données
    df = load_data()
    
    if df is None:
        st.error("Impossible de charger les données. Veuillez vérifier la connexion.")
        return
    
    # Sidebar avec logo et filtres
    with st.sidebar:
        # Logo dans la sidebar
        st.markdown("---")
        load_logo()
        st.markdown("---")
        
        st.header("🔍 Filtres")
    
    # Sélection de la page
    page = st.sidebar.selectbox(
        "📋 Sélectionner une page",
        ["🏠 Accueil", "📊 Tableau de Bord", "📈 Analyses Détaillées"]
    )
    
    # Filtres communs
    st.sidebar.subheader("Filtres de Données")
    
    # Filtre par date - utiliser date_reference qui combine date_ouverture et created_on
    date_min = pd.to_datetime(df['date_reference']).min().date()
    date_max = pd.to_datetime(df['date_reference']).max().date()
    
    date_range = st.sidebar.date_input(
        "📅 Période d'analyse",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max
    )
    
    # Filtre par marque
    marques = st.sidebar.multiselect(
        "🏷️ Marques",
        options=sorted(df['marque'].unique()),
        default=sorted(df['marque'].unique())[:10]
    )
    
    # Filtre par segment
    segments = st.sidebar.multiselect(
        "📋 Segments",
        options=sorted(df['segment'].unique()),
        default=sorted(df['segment'].unique())
    )
    
    # Filtre par zone
    zones = st.sidebar.multiselect(
        "🌍 Zones",
        options=sorted(df['zone'].dropna().unique()),
        default=sorted(df['zone'].dropna().unique())
    )
    
    # Application des filtres
    df_filtered = df.copy()
    
    # CORRECTION: Filtrage des dates avec date_reference
    if len(date_range) == 2:
        # Convertir les dates de filtrage en datetime pour la comparaison
        start_date = pd.to_datetime(date_range[0])
        end_date = pd.to_datetime(date_range[1])
        
        # Filtrer avec les dates converties en utilisant date_reference
        df_filtered = df_filtered[
            (pd.to_datetime(df_filtered['date_reference']) >= start_date) &
            (pd.to_datetime(df_filtered['date_reference']) <= end_date)
        ]
    
    if marques:
        df_filtered = df_filtered[df_filtered['marque'].isin(marques)]
    
    if segments:
        df_filtered = df_filtered[df_filtered['segment'].isin(segments)]
    
    if zones:
        df_filtered = df_filtered[df_filtered['zone'].isin(zones)]
    
    # Affichage selon la page sélectionnée
    if page == "🏠 Accueil":
        display_home_page(df, df_filtered)
    elif page == "📊 Tableau de Bord":
        display_dashboard(df_filtered)
    elif page == "📈 Analyses Détaillées":
        display_detailed_analysis(df_filtered)

def display_home_page(df, df_filtered):
    """Page d'accueil avec résumé des données"""
    st.header("🏠 Accueil - Vue d'ensemble")
    
    # KPIs globaux
    kpis = calculate_kpis(df_filtered)
    display_kpis(kpis)
    
    # Résumé des données
    st.subheader("📋 Résumé des Données")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Statistiques Générales")
        st.write(f"**Période d'analyse :** {df['date_reference'].min().strftime('%Y-%m-%d')} au {df['date_reference'].max().strftime('%Y-%m-%d')}")
        st.write(f"**Nombre total d'observations :** {len(df):,}")
        st.write(f"**Taux de présence global :** {(df['value'].sum() / len(df) * 100):.2f}%")
        st.write(f"**Nombre de produits :** {df['product_id'].nunique()}")
        st.write(f"**Nombre de points de vente :** {df['nom_point_vente'].nunique()}")
        st.write(f"**Note :** Les dates utilisent la date d'ouverture des points de vente quand disponible")
    
    with col2:
        st.markdown("### 🔍 Données Filtrées")
        st.write(f"**Observations filtrées :** {len(df_filtered):,}")
        if len(df_filtered) > 0:
            st.write(f"**Taux de présence filtré :** {(df_filtered['value'].sum() / len(df_filtered) * 100):.2f}%")
        else:
            st.write("**Taux de présence filtré :** 0.00%")
        st.write(f"**Marques sélectionnées :** {df_filtered['marque'].nunique()}")
        st.write(f"**Segments sélectionnés :** {df_filtered['segment'].nunique()}")
        st.write(f"**Zones sélectionnées :** {df_filtered['zone'].nunique()}")
    
    # Aperçu des données avec statistiques sur les dates
    st.subheader("👁️ Aperçu des Données")
    
    # Statistiques sur les sources de dates
    if len(df_filtered) > 0:
        col_stats1, col_stats2 = st.columns(2)
        
        with col_stats1:
            st.info("📊 **Statistiques des dates utilisées**")
            total_obs = len(df_filtered)
            obs_avec_date_ouverture = len(df_filtered[df_filtered['date_ouverture'].notna()])
            obs_avec_created_on = total_obs - obs_avec_date_ouverture
            
            st.write(f"• **Observations avec date d'ouverture :** {obs_avec_date_ouverture:,} ({obs_avec_date_ouverture/total_obs*100:.1f}%)")
            st.write(f"• **Observations avec date de création :** {obs_avec_created_on:,} ({obs_avec_created_on/total_obs*100:.1f}%)")
        
        with col_stats2:
            st.info("🗓️ **Étendue temporelle**")
            st.write(f"• **Date la plus ancienne :** {df_filtered['date_reference'].min().strftime('%Y-%m-%d')}")
            st.write(f"• **Date la plus récente :** {df_filtered['date_reference'].max().strftime('%Y-%m-%d')}")
            st.write(f"• **Nombre de jours couverts :** {(df_filtered['date_reference'].max() - df_filtered['date_reference'].min()).days}")
        
        st.dataframe(df_filtered.head(1000), use_container_width=True)
    else:
        st.warning("Aucune donnée ne correspond aux filtres sélectionnés.")

def display_dashboard(df_filtered):
    """Page du tableau de bord principal"""
    st.header("📊 Tableau de Bord Principal")
    
    if len(df_filtered) == 0:
        st.warning("Aucune donnée ne correspond aux filtres sélectionnés.")
        return
    
    # KPIs
    kpis = calculate_kpis(df_filtered)
    display_kpis(kpis)
    
    # Graphiques principaux
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        fig_brand = create_brand_chart(df_filtered)
        st.plotly_chart(fig_brand, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        fig_segment = create_segment_chart(df_filtered)
        st.plotly_chart(fig_segment, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Graphique temporel
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    fig_time = create_time_chart(df_filtered)
    st.plotly_chart(fig_time, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Carte géographique
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    fig_geo = create_geo_chart(df_filtered)
    if fig_geo:
        st.plotly_chart(fig_geo, use_container_width=True)
    else:
        st.info("Données de géolocalisation insuffisantes pour afficher la carte.")
    st.markdown('</div>', unsafe_allow_html=True)

def display_detailed_analysis(df_filtered):
    """Page d'analyses détaillées"""
    st.header("📈 Analyses Détaillées")
    
    if len(df_filtered) == 0:
        st.warning("Aucune donnée ne correspond aux filtres sélectionnés.")
        return
    
    # Analyses par produit
    st.subheader("🛍️ Analyse par Produit")
    
    product_stats = df_filtered.groupby(['nom_produit', 'marque']).agg({
        'value': ['count', 'sum', 'mean']
    }).round(3)
    
    product_stats.columns = ['Observations', 'Presences', 'Taux_Presence']
    product_stats = product_stats.reset_index().sort_values('Taux_Presence', ascending=False)
    
    # Top 10 et Bottom 10
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏆 Top 10 - Meilleurs Produits")
        top_products = product_stats.head(10)
        st.dataframe(top_products, use_container_width=True)
    
    with col2:
        st.markdown("#### 🔻 Bottom 10 - Produits à Améliorer")
        bottom_products = product_stats.tail(10)
        st.dataframe(bottom_products, use_container_width=True)
    
    # Analyse par zone géographique
    st.subheader("🌍 Analyse par Zone Géographique")
    
    zone_stats = df_filtered.groupby('zone').agg({
        'value': ['count', 'sum', 'mean'],
        'nom_point_vente': 'nunique'
    }).round(3)
    
    zone_stats.columns = ['Observations', 'Presences', 'Taux_Presence', 'Nb_Points_Vente']
    zone_stats = zone_stats.reset_index().sort_values('Taux_Presence', ascending=False)
    
    # Graphique des zones
    fig_zone = px.bar(
        zone_stats,
        x='zone',
        y='Taux_Presence',
        color='Taux_Presence',
        color_continuous_scale='RdYlGn',
        title="🌍 Taux de Présence par Zone Géographique"
    )
    
    fig_zone.update_layout(xaxis_tickangle=-45, height=500)
    st.plotly_chart(fig_zone, use_container_width=True)
    
    # Tableau détaillé des zones
    st.dataframe(zone_stats, use_container_width=True)

if __name__ == "__main__":
    main()