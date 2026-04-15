import streamlit as st
import pandas as pd
import os

# --- Configuration de la page ---
st.set_page_config(page_title="Analyse Prix Sénégal", page_icon="🛒", layout="wide")

# --- Titre ---
st.title("🛒 Dashboard : Analyse des Prix de Consommation au Sénégal")
st.markdown("Ce tableau de bord permet de suivre l'évolution des prix entre différents supermarchés (Auchan, Sakanal, etc.).")

# --- Chargement des données ---
# st.cache_data permet de ne pas recharger le CSV à chaque clic
@st.cache_data
def load_data():
    # Remplace par le nom exact de ton dernier fichier généré
    filename = "donnees_brutes_combinees_2026-04-15.csv" 
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        return df
    else:
        st.error(f"Le fichier {filename} est introuvable.")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    # --- KPIs de base en haut de la page ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total des produits collectés", value=len(df))
    with col2:
        st.metric(label="Magasins analysés", value=df['magasin'].nunique())
    with col3:
        st.metric(label="Catégories couvertes", value=df['categorie'].nunique())

    st.divider() # Une ligne de séparation

    # --- Section Graphiques ---
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Répartition des produits par Magasin")
        # On compte le nombre de produits par magasin et on affiche un graphique à barres
        repartition_magasin = df['magasin'].value_counts()
        st.bar_chart(repartition_magasin)

    with col_chart2:
        st.subheader("Prix moyen par Catégorie (FCFA)")
        # On groupe par catégorie et on calcule la moyenne des prix
        prix_moyen_cat = df.groupby('categorie')['prix'].mean().sort_values(ascending=False)
        st.bar_chart(prix_moyen_cat)

    st.divider()

    # --- Affichage du tableau de données ---
    st.subheader("🔍 Explorer les données brutes")
    
    # Ajout d'une barre de recherche simple
    recherche = st.text_input("Rechercher un produit (ex: Lait, Nido, Huile...)")
    
    if recherche:
        # Filtre le dataframe si l'utilisateur tape quelque chose (en ignorant la casse)
        df_affichee = df[df['nom_produit'].str.contains(recherche, case=False, na=False)]
    else:
        df_affichee = df
        
    st.dataframe(df_affichee)