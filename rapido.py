import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import os
import re
import tempfile
from io import BytesIO
from pathlib import Path
from PyPDF2 import PdfReader
import logging # Ajout pour un meilleur logging
# Import the main function from carburant.py
from carburant import main as module_carburant_main

# Configuration du logging (optionnel mais utile pour le debug)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

################################
# Fonctions / classes utilitaires
################################

def to_excel(df: pd.DataFrame) -> bytes:
    """Convertit un DataFrame en fichier Excel binaire en mémoire."""
    output = BytesIO()
    # Utilisation de try-except pour gérer les erreurs potentielles d'écriture Excel
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        processed_data = output.getvalue()
        return processed_data
    except Exception as e:
        st.error(f"Erreur lors de la génération du fichier Excel : {e}")
        return b"" # Retourne des bytes vides en cas d'erreur

def is_non_working_hour(dt: datetime, start_non_work: int, end_non_work: int) -> bool:
    """
    Détermine si l'heure de l'objet datetime est en dehors des heures ouvrées.
    Les heures ouvrées sont de end_non_work (inclus) à start_non_work (exclus).
    """
    h = dt.hour
    # Cas standard: ex: start=18, end=8. Heures non ouvrées: >= 18 OU < 8
    if start_non_work > end_non_work:
        return h >= start_non_work or h < end_non_work
    # Cas où les heures non ouvrées sont dans la même journée: ex: start=8, end=17. Non ouvrées: >= 8 ET < 17
    elif start_non_work < end_non_work:
         return start_non_work <= h < end_non_work
    # Cas où start == end, on suppose que tout est ouvré (ou non ouvré si on veut, ici on dit ouvré)
    else:
        return False

def is_weekend(dt: datetime) -> bool:
    """Vérifie si la date est un Samedi (5) ou Dimanche (6)."""
    return dt.weekday() >= 5

class AutorouteAnalyzer:
    def __init__(self):
        self.all_transactions = []
        self.card_owners = {}
        self.raw_df = pd.DataFrame()
        self.analysis_results = {}
        self.start_non_work_hour = 18 # Default value
        self.end_non_work_hour = 8    # Default value
        self.is_data_loaded = False

    def set_non_working_hours(self, start: int, end: int):
        """Définit les heures non ouvrées."""
        self.start_non_work_hour = start
        self.end_non_work_hour = end
        # Recalculer si les données sont déjà chargées
        if not self.raw_df.empty:
            self._enrich_dataframe()

    def load_card_owners(self, excel_file_path: str):
        """Charge la liste des propriétaires de cartes depuis un fichier Excel."""
        try:
            # S'assurer que le type est bien str pour éviter les problèmes avec les numéros longs
            df = pd.read_excel(excel_file_path, dtype={'N°CARTERAPIDO': str})
            # Nettoyage potentiel des espaces ou caractères non visibles
            df['N°CARTERAPIDO'] = df['N°CARTERAPIDO'].astype(str).str.strip()
            df.dropna(subset=['N°CARTERAPIDO', 'USER'], inplace=True) # Ignorer lignes avec N° ou USER manquant
            self.card_owners = df.set_index('N°CARTERAPIDO')['USER'].to_dict()
            logging.info(f"Chargé {len(self.card_owners)} propriétaires de cartes depuis {excel_file_path}")
        except FileNotFoundError:
            st.error(f"Erreur: Le fichier Excel '{excel_file_path}' n'a pas été trouvé.")
            self.card_owners = {}
        except Exception as e:
            st.error(f"Erreur lors du chargement ou de la lecture du fichier Excel des cartes: {e}")
            logging.error(f"Erreur chargement liste des cartes: {e}", exc_info=True)
            self.card_owners = {}

    def parse_pdf_content(self, pdf_content: BytesIO, account_number: str):
        """Parse le contenu d'un fichier PDF et extrait les transactions."""
        try:
            reader = PdfReader(pdf_content)
            text = ""
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"

            # Regex plus robuste pour capturer la ligne de transaction
            # Format: JJ/MM/AAAA HH:MM:SS TYPE ... MONTANT XOF ...
            # Le montant peut avoir des points comme séparateurs de milliers et une virgule décimale
            transaction_pattern = re.compile(
                r"(\d{2}/\d{2}/\d{4})\s+"       # Date
                r"(\d{2}:\d{2}:\d{2})\s+"       # Heure
                r"(\S+)\s+"                     # Type (non-space characters)
                r"(.*?)\s+"                     # Reste de la ligne (non-greedy)
                r"([\d.,]+)\s+XOF"              # Montant suivi de XOF
            )

            account_number = str(account_number).strip()
            direction = self.card_owners.get(account_number, "Direction Non Identifiée")
            parsed_count = 0

            for line in text.split('\n'):
                match = transaction_pattern.search(line)
                if match:
                    date_str, time_str, movement_type, _, amount_str = match.groups()
                    date_time_str = f"{date_str} {time_str}"
                    amount = 0.0
                    try:
                        # Nettoyer le montant: supprimer les points, remplacer la virgule par un point
                        amount = float(amount_str.replace('.', '').replace(',', '.'))
                    except ValueError:
                        logging.warning(f"Impossible de parser le montant '{amount_str}' pour le compte {account_number} sur la ligne: {line}")
                        amount = 0.0 # Ou choisir de lever une erreur ou skipper

                    try:
                        dt = datetime.strptime(date_time_str, '%d/%m/%Y %H:%M:%S')
                        transaction = {
                            'account': account_number,
                            'datetime': dt,
                            'type': movement_type.capitalize(), # Standardiser la casse
                            'amount': amount,
                            'direction': direction if direction else "Direction Non Identifiée"
                        }
                        self.all_transactions.append(transaction)
                        parsed_count += 1
                    except ValueError:
                        logging.warning(f"Format de date/heure invalide '{date_time_str}' pour le compte {account_number} sur la ligne: {line}")
                        continue # Ignore cette ligne si la date n'est pas valide

            logging.info(f"PDF {account_number}: {parsed_count} transactions parsées.")

        except Exception as e:
            st.error(f"Erreur lors du parsing du PDF pour le compte {account_number}: {e}")
            logging.error(f"Erreur parsing PDF {account_number}: {e}", exc_info=True)

    def _enrich_dataframe(self):
        """Ajoute des colonnes calculées au DataFrame brut."""
        if self.raw_df.empty:
            return

        df = self.raw_df # Travaille sur la référence interne

        if 'datetime' not in df.columns:
             logging.error("La colonne 'datetime' est manquante dans le DataFrame brut.")
             st.error("Erreur interne: Colonne 'datetime' non trouvée.")
             return

        df['date'] = df['datetime'].dt.date
        df['hour'] = df['datetime'].dt.hour
        df['month'] = df['datetime'].dt.strftime('%Y-%m')
        df['year'] = df['datetime'].dt.year
        df['day_of_week_num'] = df['datetime'].dt.weekday # Pour tri
        df['is_weekend'] = df['datetime'].apply(is_weekend)

        jours_fr = { 0: 'Lundi', 1: 'Mardi', 2: 'Mercredi', 3: 'Jeudi', 4: 'Vendredi', 5: 'Samedi', 6: 'Dimanche'}
        df['day_of_week'] = df['day_of_week_num'].map(jours_fr)

        # Appliquer le calcul basé sur les heures définies dans l'instance
        df['is_non_working_hours'] = df['datetime'].apply(
            lambda dt: is_non_working_hour(dt, self.start_non_work_hour, self.end_non_work_hour)
        )

        # Assigner le DataFrame enrichi à l'attribut de classe
        self.raw_df = df


    def finalize_data_loading(self):
        """Crée le DataFrame final à partir des transactions parsées et l'enrichit."""
        if not self.all_transactions:
            st.warning("Aucune transaction n'a été trouvée ou parsée.")
            self.raw_df = pd.DataFrame()
            self.is_data_loaded = False
            return

        self.raw_df = pd.DataFrame(self.all_transactions)
        # Conversion explicite en datetime si ce n'est pas déjà fait
        self.raw_df['datetime'] = pd.to_datetime(self.raw_df['datetime'])
        # Tri par date pour la cohérence
        self.raw_df.sort_values(by='datetime', inplace=True)
        self._enrich_dataframe() # Enrichir avec les colonnes calculées
        self.is_data_loaded = True
        logging.info(f"DataFrame final créé avec {len(self.raw_df)} transactions.")


    def run_all_analyses(self, inactivity_threshold=90):
        """Exécute toutes les analyses et stocke les résultats."""
        if self.raw_df.empty:
            st.warning("Impossible d'exécuter les analyses : aucune donnée chargée.")
            self.analysis_results = {}
            return {}

        df = self.raw_df # Utilise le DF enrichi de la classe

        # Utilisation de méthodes préfixées par _ pour indiquer qu'elles sont "privées" à la classe
        self.analysis_results['passages_mensuels_par_direction'] = self._analyze_passages_mensuels_par_direction(df)
        self.analysis_results['nb_passages_par_carte_direction'] = self._analyze_nb_passages_par_carte_direction(df)
        self.analysis_results['nb_cartes_par_direction'] = self._analyze_nb_cartes_par_direction(df)
        self.analysis_results['cartes_weekend_heures_non_ouvrees'] = self._analyze_cartes_weekend_heures_non_ouvrees(df)

        # Analyse d'activité
        activite_summary, activite_details = self._analyze_card_activity(df, inactivity_threshold)
        self.analysis_results['activite_cartes_resume'] = activite_summary
        self.analysis_results['activite_cartes_details'] = activite_details

        # Analyses supplémentaires
        self.analysis_results['recharges_mensuelles'] = self._analyze_recharges_mensuelles(df)
        self.analysis_results['recharges_par_direction'] = self._analyze_recharges_par_direction(df)
        self.analysis_results['heures_pointe_par_direction'] = self._analyze_peak_hours_per_direction(df)

        logging.info("Toutes les analyses ont été exécutées.")
        return self.analysis_results

    def get_filtered_data(self, start_date=None, end_date=None, directions=None, types=None, weekend_filter=None, non_working_hours_filter=None, search_term=None):
        """Filtre le DataFrame principal selon les critères fournis."""
        if self.raw_df.empty:
            return pd.DataFrame()

        filtered_df = self.raw_df.copy()

        if start_date:
            filtered_df = filtered_df[filtered_df['datetime'].dt.date >= start_date]
        if end_date:
            filtered_df = filtered_df[filtered_df['datetime'].dt.date <= end_date]
        if directions and "Tous" not in directions:
            filtered_df = filtered_df[filtered_df['direction'].isin(directions)]
        if types:
            filtered_df = filtered_df[filtered_df['type'].isin(types)]

        # Filtre Weekend / Heures non ouvrées (peut être 'Oui', 'Non', ou None/Tous)
        if weekend_filter == 'Oui':
             filtered_df = filtered_df[filtered_df['is_weekend']]
        elif weekend_filter == 'Non':
             filtered_df = filtered_df[~filtered_df['is_weekend']]

        if non_working_hours_filter == 'Oui':
             filtered_df = filtered_df[filtered_df['is_non_working_hours']]
        elif non_working_hours_filter == 'Non':
             filtered_df = filtered_df[~filtered_df['is_non_working_hours']]

        # Combinaison des filtres Weekend ET/OU Heures non ouvrées si nécessaire
        # Exemple: Si l'utilisateur sélectionne "Weekend" ET "Heures non ouvrées" dans un multiselect
        # on pourrait vouloir les transactions qui sont SOIT l'un SOIT l'autre.
        # La logique actuelle filtre séquentiellement. Adapter si besoin.

        if search_term:
            filtered_df = filtered_df[
                filtered_df['account'].astype(str).str.contains(search_term, case=False, na=False) |
                filtered_df['amount'].astype(str).str.contains(search_term, case=False, na=False) # Recherche aussi sur le montant
            ]

        return filtered_df

    # --- Méthodes d'Analyse Privées ---

    def _analyze_card_activity(self, df: pd.DataFrame, inactivity_threshold=90):
        """Analyse l'activité des cartes."""
        if df.empty:
            # Retourne des DataFrames vides avec les colonnes attendues si aucune donnée
            summary_cols = ['Direction', 'Total_cartes', 'Cartes_actives', 'Cartes_inactives']
            details_cols = ['Numéro_carte_Rapido', 'Direction', 'Statut', 'Dernière_transaction', 'Jours_depuis_derniere_transaction']
            return pd.DataFrame(columns=summary_cols), pd.DataFrame(columns=details_cols)

        # Utiliser la date maximale des transactions comme référence si disponible, sinon aujourd'hui
        reference_date = df['datetime'].max() if not df.empty else pd.Timestamp.now()
        reference_date = pd.to_datetime(reference_date) # Ensure it's a Timestamp

        all_known_cards = set(self.card_owners.keys())
        active_cards_in_df = set(df['account'].unique())

        # Cartes connues mais sans transaction dans le df actuel
        inactive_cards = all_known_cards - active_cards_in_df

        # Préparer les détails pour TOUTES les cartes connues
        details_data = []
        for card in all_known_cards:
            card_transactions = df[df['account'] == card]
            direction = self.card_owners.get(card, "Inconnu")
            last_transaction_time = None
            days_since_last = None
            status = "Inactive (jamais vue)" # Statut initial

            if not card_transactions.empty:
                last_transaction_time = card_transactions['datetime'].max()
                days_since_last = (reference_date - last_transaction_time).days
                if days_since_last <= inactivity_threshold:
                     status = "Active"
                else:
                     status = f"Inactive ({days_since_last} j)" # Plus précis
            elif card in inactive_cards:
                 # La carte est connue mais n'a *aucune* transaction dans le DF actuel.
                 # On ne peut pas calculer "days_since_last" à partir de ce DF.
                 # On pourrait chercher la dernière date dans *toutes* les données si nécessaire,
                 # mais ici on se base sur le DF filtré/passé en argument.
                 status = "Inactive (période)" # Ou "Inactive (jamais vue)" si all_transactions est vide

            details_data.append({
                'Numéro_carte_Rapido': card,
                'Direction': direction,
                'Statut': status,
                'Dernière_transaction': last_transaction_time.date() if pd.notna(last_transaction_time) else None,
                'Jours_depuis_derniere_transaction': days_since_last if pd.notna(days_since_last) else None
            })

        details_df = pd.DataFrame(details_data)

        # Calculer le résumé basé sur les statuts déterminés
        summary_data = []
        # S'assurer que 'Direction' existe dans details_df
        if 'Direction' in details_df.columns:
            for direction in sorted(details_df['Direction'].unique()):
                dir_subset = details_df[details_df['Direction'] == direction]
                total_dir = len(dir_subset)
                active_dir = len(dir_subset[dir_subset['Statut'] == "Active"])
                inactive_dir = total_dir - active_dir
                summary_data.append({
                    'Direction': direction,
                    'Total_cartes': total_dir,
                    'Cartes_actives': active_dir,
                    'Cartes_inactives': inactive_dir
                })
        else:
            # Gérer le cas où details_df pourrait être vide ou sans colonne Direction
             logging.warning("Impossible de générer le résumé d'activité par direction.")


        # Ajouter une ligne 'Total'
        if not details_df.empty:
             total_cards = len(details_df)
             total_active = len(details_df[details_df['Statut'] == "Active"])
             total_inactive = total_cards - total_active
             summary_data.append({
                 'Direction': 'Total',
                 'Total_cartes': total_cards,
                 'Cartes_actives': total_active,
                 'Cartes_inactives': total_inactive
             })

        summary_df = pd.DataFrame(summary_data)

        return summary_df, details_df

    def _analyze_passages_mensuels_par_direction(self, df: pd.DataFrame):
        if df.empty or 'direction' not in df.columns or 'month' not in df.columns:
             return pd.DataFrame(columns=['direction', 'month', 'passages', 'nombre_passages_total']) # Structure vide
        group = df.groupby(['direction', 'month']).size().reset_index(name='passages')
        # Utiliser pivot_table pour gérer les mois manquants pour certaines directions
        pivot = pd.pivot_table(group, index='direction', columns='month', values='passages', fill_value=0)
        pivot['nombre_passages_total'] = pivot.sum(axis=1)
        pivot.reset_index(inplace=True)
        return pivot

    def _analyze_nb_passages_par_carte_direction(self, df: pd.DataFrame):
        if df.empty:
            return pd.DataFrame(columns=['Numéro carte Rapido', 'Direction', 'Nombre_passages_Transit', 'Montant_Transit', 'Montant_Promotion', 'Montant_Recharge'])

        # Filtrer par type AVANT de grouper
        df_transit = df[df['type'].str.lower() == 'transit']
        df_promo = df[df['type'].str.lower() == 'promotion']
        df_recharge = df[df['type'].str.lower() == 'recharge']

        # Aggregations
        transit_agg = df_transit.groupby(['account', 'direction']).agg(
            Nombre_passages_Transit=('datetime', 'size'), # Utilise une colonne non-NA
            Montant_Transit=('amount', 'sum')
        ).reset_index()

        promo_sum = df_promo.groupby(['account', 'direction'])['amount'].sum().reset_index(name='Montant_Promotion')
        recharge_sum = df_recharge.groupby(['account', 'direction'])['amount'].sum().reset_index(name='Montant_Recharge')

        # Merge en partant de toutes les combinaisons compte/direction vues dans transit (ou dans toutes les transactions si besoin)
        # Ici on part de transit_agg qui contient déjà les comptes/directions ayant eu des transits
        merged = transit_agg
        if not promo_sum.empty:
            merged = pd.merge(merged, promo_sum, on=['account', 'direction'], how='left')
        else:
            merged['Montant_Promotion'] = 0

        if not recharge_sum.empty:
            merged = pd.merge(merged, recharge_sum, on=['account', 'direction'], how='left')
        else:
            merged['Montant_Recharge'] = 0

        merged.fillna(0, inplace=True)

        merged.rename(columns={'account': 'Numéro carte Rapido', 'direction': 'Direction'}, inplace=True)
        return merged[['Numéro carte Rapido', 'Direction', 'Nombre_passages_Transit', 'Montant_Transit', 'Montant_Promotion', 'Montant_Recharge']]

    def _analyze_nb_cartes_par_direction(self, df: pd.DataFrame):
         if df.empty or 'account' not in df.columns or 'direction' not in df.columns:
             return pd.DataFrame(columns=['direction', 'nb_cartes'])
         # Compte les cartes uniques par direction basées sur les transactions présentes dans le df
         unique_combo = df[['account', 'direction']].drop_duplicates()
         grouped = unique_combo.groupby('direction').size().reset_index(name='nb_cartes_actives_periode') # Nom plus précis
         grouped.rename(columns={'direction': 'Direction'}, inplace=True)
         return grouped

    def _analyze_cartes_weekend_heures_non_ouvrees(self, df: pd.DataFrame):
        if df.empty:
            return pd.DataFrame(columns=['Numéro carte Rapido', 'Direction', 'Montant_Transit_Weekend', 'nb_weekend_transit', 'Montant_Transit_HH', 'nb_hors_horaires_transit'])

        # Filtrer uniquement les transactions de type 'Transit'
        df_transit = df[df['type'].str.lower() == 'transit'].copy()
        if df_transit.empty:
             return pd.DataFrame(columns=['Numéro carte Rapido', 'Direction', 'Montant_Transit_Weekend', 'nb_weekend_transit', 'Montant_Transit_HH', 'nb_hors_horaires_transit'])


        # Transactions Transit pendant le weekend
        weekend = df_transit[df_transit['is_weekend']]
        weekend_group = weekend.groupby(['account', 'direction']).agg(
            Montant_Transit_Weekend=('amount', 'sum'),
            nb_weekend_transit=('datetime', 'count') # ou 'size'
        ).reset_index()

        # Transactions Transit en heures non ouvrées ET PAS le weekend
        hh = df_transit[df_transit['is_non_working_hours'] & (~df_transit['is_weekend'])]
        hh_group = hh.groupby(['account', 'direction']).agg(
            Montant_Transit_HH=('amount', 'sum'),
            nb_hors_horaires_transit=('datetime', 'count')
        ).reset_index()

        # Fusionner les deux, en partant de toutes les cartes/directions ayant eu au moins une transaction WE ou HH
        # Utiliser une fusion externe pour garder toutes les cartes
        merged_wehh = pd.merge(weekend_group, hh_group, on=['account', 'direction'], how='outer')
        merged_wehh.fillna(0, inplace=True)
        merged_wehh.rename(columns={'account': 'Numéro carte Rapido', 'direction': 'Direction'}, inplace=True)

        # S'assurer que toutes les colonnes existent même si l'un des groupes est vide
        for col in ['Montant_Transit_Weekend', 'nb_weekend_transit', 'Montant_Transit_HH', 'nb_hors_horaires_transit']:
             if col not in merged_wehh.columns:
                 merged_wehh[col] = 0

        return merged_wehh[['Numéro carte Rapido', 'Direction', 'Montant_Transit_Weekend', 'nb_weekend_transit', 'Montant_Transit_HH', 'nb_hors_horaires_transit']]

    def _analyze_recharges_mensuelles(self, df: pd.DataFrame):
        """Calcule le montant total des recharges par mois."""
        if df.empty or 'type' not in df.columns or 'month' not in df.columns or 'amount' not in df.columns:
            return pd.Series(dtype=float).rename('Montant_Recharge')
        recharges = df[df['type'].str.lower() == 'recharge']
        monthly_recharges = recharges.groupby('month')['amount'].sum().rename('Montant_Recharge')
        return monthly_recharges

    def _analyze_recharges_par_direction(self, df: pd.DataFrame):
        """Calcule le montant total des recharges par direction."""
        if df.empty or 'type' not in df.columns or 'direction' not in df.columns or 'amount' not in df.columns:
            return pd.DataFrame(columns=['Direction', 'Montant total des recharges'])
        recharges = df[df['type'].str.lower() == 'recharge']
        recharge_by_direction = recharges.groupby('direction')['amount'].sum().reset_index()
        recharge_by_direction.rename(columns={'direction': 'Direction', 'amount': 'Montant total des recharges'}, inplace=True)
        return recharge_by_direction

    def _analyze_peak_hours_per_direction(self, df: pd.DataFrame):
        """Analyse les heures de pointe (nombre de transactions) par direction."""
        if df.empty or 'direction' not in df.columns or 'hour' not in df.columns:
             return pd.DataFrame() # Retourne un DF vide
        # Compte les transactions par direction et par heure
        peak_hours = df.groupby(['direction', 'hour']).size().reset_index(name='transactions')
        # Pivoter pour avoir les heures en colonnes
        peak_hours_pivot = pd.pivot_table(peak_hours, index='direction', columns='hour', values='transactions', fill_value=0)
        # S'assurer que toutes les heures de 0 à 23 sont présentes
        all_hours = list(range(24))
        peak_hours_pivot = peak_hours_pivot.reindex(columns=all_hours, fill_value=0)
        return peak_hours_pivot

    # --- Export ---
    def export_analyses_to_excel(self, filename: str, start_date: date = None, end_date: date = None, inactivity_threshold=90):
        """
        Exporte les données brutes filtrées et toutes les analyses (recalculées sur les données filtrées)
        vers un fichier Excel multi-feuilles.
        """
        if self.raw_df.empty:
            st.error("Aucune donnée à exporter.")
            return

        # 1. Filtrer les données brutes selon la plage de dates
        export_df = self.get_filtered_data(start_date=start_date, end_date=end_date)

        if export_df.empty:
             st.warning(f"Aucune transaction trouvée pour la période du {start_date} au {end_date}. L'export sera vide.")
             # Créer un fichier Excel vide ou avec juste un message? Ici on arrête.
             return

        # 2. Recalculer TOUTES les analyses sur ce DataFrame filtré
        #    C'est coûteux mais garantit que l'export reflète exactement la période sélectionnée.
        #    Alternative: Exporter les analyses pré-calculées et indiquer la période globale de ces analyses.
        #    Ici, on choisit de recalculer pour la précision de l'export par période.
        temp_analyzer = AutorouteAnalyzer() # Créer une instance temporaire pour ne pas affecter l'état principal
        temp_analyzer.raw_df = export_df.copy() # Utiliser les données filtrées
        temp_analyzer.card_owners = self.card_owners # Réutiliser la liste des cartes
        # Pas besoin de re-parser, juste recalculer les analyses
        export_analyses = temp_analyzer.run_all_analyses(inactivity_threshold=inactivity_threshold)


        # 3. Préparer les DataFrames pour l'export (Renommage des colonnes, etc.)
        renamed_raw = export_df.rename(columns={
            'account': 'Numéro carte Rapido', 'datetime': 'Date et heure', 'type': 'Type transaction',
            'amount': 'Montant', 'direction': 'Direction', 'date': 'Date', 'hour': 'Heure',
            'month': 'Mois', 'is_weekend': 'Weekend', 'is_non_working_hours': 'Hors heures ouvrées',
            'day_of_week': 'Jour de la semaine', 'year': 'Année', 'day_of_week_num': 'Num Jour Semaine'
        })
        # Sélectionner/réorganiser les colonnes pour l'export brut si nécessaire
        cols_to_export = [
            'Numéro carte Rapido', 'Direction', 'Date et heure', 'Date', 'Heure', 'Jour de la semaine',
            'Type transaction', 'Montant', 'Mois', 'Année', 'Weekend', 'Hors heures ouvrées'
        ]
        # Garder seulement les colonnes qui existent réellement dans le DF renommé
        renamed_raw_export = renamed_raw[[col for col in cols_to_export if col in renamed_raw.columns]]


        # 4. Écrire dans le fichier Excel
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                renamed_raw_export.to_excel(writer, sheet_name='Transactions_Filtrees', index=False)
                for sheet_name, data in export_analyses.items():
                    # Gérer les Series (comme recharges_mensuelles) et DataFrames
                    if isinstance(data, pd.DataFrame):
                        if not data.empty:
                           # Remplacer les caractères non valides pour les noms de feuilles Excel si nécessaire
                           safe_sheet_name = re.sub(r'[\\/*?:\[\]]', '_', sheet_name)[:31] # Limite Excel
                           data.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    elif isinstance(data, pd.Series):
                         if not data.empty:
                             safe_sheet_name = re.sub(r'[\\/*?:\[\]]', '_', sheet_name)[:31]
                             data.to_frame().to_excel(writer, sheet_name=safe_sheet_name, index=True) # Garder l'index (ex: mois)
            logging.info(f"Export Excel '{filename}' généré avec succès pour la période {start_date} à {end_date}.")
        except Exception as e:
             st.error(f"Erreur lors de l'écriture du fichier Excel '{filename}': {e}")
             logging.error(f"Erreur écriture Excel '{filename}': {e}", exc_info=True)


################################
# Mise en page et Styles (identique à l'original)
################################
st.set_page_config(
    page_title="Analyse des passages Rapido",
    page_icon="🚗",
    layout="wide"
)

# Ajout d'un style CSS pour améliorer l'apparence
st.markdown("""
<style>
    /* Main app background */
    .main {
        background-color: #f9f9f9; /* Light grey background */
        padding: 1rem 2rem; /* More padding */
    }

    /* Card-like containers for charts and tables */
    .stDataFrame, .stPlotlyChart {
        background-color: white;
        border-radius: 8px; /* Softer corners */
        padding: 1rem 1.5rem; /* Consistent padding */
        box-shadow: 0 4px 8px rgba(0,0,0,0.05); /* Softer shadow */
        margin-bottom: 1.5rem; /* Space between elements */
        border: 1px solid #e0e0e0; /* Subtle border */
    }

    /* Sidebar styling */
    .sidebar .sidebar-content {
        background-color: #e8f0f4; /* Light blue-grey */
        padding: 1.5rem;
    }
    .sidebar .stFileUploader, .sidebar .stSlider, .sidebar .stMultiSelect {
        margin-bottom: 1rem;
    }
    .sidebar h3 {
        color: #004080; /* Dark blue */
        margin-top: 1.5rem;
    }

    /* Titles and Headers */
    h1, h2, h3 {
        color: #0056b3; /* Consistent blue for titles */
    }
    h1 {
        border-bottom: 2px solid #0056b3;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem;
    }
    h2 {
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    h3 {
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
    }

    /* Buttons */
    .stButton>button {
        background-color: #007bff;
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        border: none;
        transition: background-color 0.2s ease;
    }
    .stButton>button:hover {
        background-color: #0056b3;
    }
    .stDownloadButton>button {
        background-color: #28a745; /* Green for download */
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        border: none;
        transition: background-color 0.2s ease;
    }
    .stDownloadButton>button:hover {
        background-color: #218838;
    }

    /* Metrics styling */
    .stMetric {
        background-color: #ffffff;
        border-left: 5px solid #007bff; /* Blue accent line */
        padding: 1rem;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        margin-bottom: 1rem; /* Add space below metrics */
    }
    .stMetric > div:nth-child(1) { /* Label */
        color: #555;
    }
    .stMetric > div:nth-child(2) { /* Value */
        font-size: 1.5em;
        font-weight: bold;
        color: #0056b3;
    }
     .stMetric > div:nth-child(3) { /* Delta (optional) */
        color: #28a745; /* Green delta, adjust if needed */
    }

    /* Specific info/warning boxes */
    .custom-info-box {
        padding: 10px;
        border-radius: 5px;
        background-color: #e0e0e0; /* Light grey */
        margin-bottom: 10px;
        border: 1px solid #c0c0c0;
    }
    .custom-status-active {
        color: #28a745; /* Green */
        font-weight: bold;
    }
     .custom-status-inactive {
        color: #dc3545; /* Red */
        font-weight: bold;
    }


    /* Copyright Footer */
    .footer {
        text-align: center;
        color: gray;
        margin-top: 3rem;
        padding: 1rem;
        font-size: 0.9em;
        border-top: 1px solid #e0e0e0;
    }
    .security-info {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-top: 10px;
        border: 1px solid #e9ecef;
        display: flex;
        align-items: center;
        justify-content: center; /* Center content */
        margin-bottom: 20px; /* Add space below */
    }
     .security-info span:first-child { /* Icon */
        color: #1f77b4;
        font-size: 1.2em;
        margin-right: 8px;
    }
     .security-info span:last-child { /* Text */
        color: #666;
        font-size: 0.9em;
    }

</style>
""", unsafe_allow_html=True)


# ===============================================
# Fonctions de Visualisation (Peu de changements nécessaires ici)
# ===============================================

def create_monthly_trend_chart(df: pd.DataFrame):
    """Crée un graphique linéaire de la tendance mensuelle des transactions."""
    if df.empty or 'month' not in df.columns:
        return go.Figure().update_layout(title='Aucune donnée pour la tendance mensuelle', xaxis={'visible': False}, yaxis={'visible': False})
    monthly_data = df.groupby('month').size().reset_index(name='transactions')
    fig = px.line(monthly_data, x='month', y='transactions',
                  title='Tendance Mensuelle (Transactions)',
                  labels={'transactions': 'Nb Transactions', 'month': 'Mois'},
                  markers=True) # Ajoute des marqueurs pour la visibilité
    fig.update_layout(height=350, title_x=0.5) # Centrer le titre
    return fig

def create_direction_pie_chart(df: pd.DataFrame):
    """Crée un diagramme circulaire de la répartition par direction."""
    if df.empty or 'direction' not in df.columns:
        return go.Figure().update_layout(title='Aucune donnée pour la répartition par direction', xaxis={'visible': False}, yaxis={'visible': False})
    direction_data = df.groupby('direction').size().reset_index(name='transactions')
    fig = px.pie(direction_data, names='direction', values='transactions',
                 title='Répartition par Direction (Transactions)',
                 hole=0.3) # Donne un effet Donut
    fig.update_layout(showlegend=True, height=350, title_x=0.5)
    return fig

def create_weekday_bar_chart(df: pd.DataFrame):
    """Crée un graphique en barres des transactions par jour de la semaine."""
    if df.empty or 'day_of_week' not in df.columns:
        return go.Figure().update_layout(title='Aucune donnée par jour de semaine', xaxis={'visible': False}, yaxis={'visible': False})
    weekday_data = df.groupby('day_of_week').size().reset_index(name='transactions')
    ordre_jours = ['Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi','Dimanche']
    weekday_data['day_of_week'] = pd.Categorical(weekday_data['day_of_week'],
                                                 categories=ordre_jours,
                                                 ordered=True)
    weekday_data = weekday_data.sort_values('day_of_week')

    fig = px.bar(weekday_data, x='day_of_week', y='transactions',
                 title='Transactions par Jour de la Semaine',
                 labels={'transactions': 'Nb Transactions', 'day_of_week': 'Jour'},
                 color='day_of_week', text_auto=True) # Affiche les valeurs sur les barres
    fig.update_layout(showlegend=False, height=350, title_x=0.5)
    fig.update_traces(textposition='outside')
    # fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide') # Peut cacher des textes si barres petites
    return fig

def create_hourly_heatmap(df: pd.DataFrame):
    """Crée une heatmap de la distribution horaire par jour."""
    if df.empty or 'day_of_week' not in df.columns or 'hour' not in df.columns:
         return go.Figure().update_layout(title='Aucune donnée pour la heatmap horaire', xaxis={'visible': False}, yaxis={'visible': False})
    hourly_data = df.groupby(['day_of_week', 'hour']).size().reset_index(name='transactions')
    pivot_data = pd.pivot_table(hourly_data, index='day_of_week', columns='hour', values='transactions', fill_value=0)

    ordre_jours = ['Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi','Dimanche']
    pivot_data = pivot_data.reindex(ordre_jours, fill_value=0) # Assure l'ordre et inclut les jours sans transaction
    # S'assurer que toutes les heures 0-23 sont présentes
    all_hours = list(range(24))
    pivot_data = pivot_data.reindex(columns=all_hours, fill_value=0)


    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=[f"{h}h" for h in pivot_data.columns],
        y=pivot_data.index,
        colorscale='Viridis',
        colorbar={'title': 'Nb Transactions'}
    ))
    fig.update_layout(
        title='Heatmap Horaire des Transactions (par Jour)',
        xaxis_title='Heure de la journée',
        yaxis_title='Jour de la semaine',
        height=400,
        title_x=0.5
    )
    return fig

def create_daily_trend_chart(df: pd.DataFrame):
    """Crée un graphique linéaire de la tendance journalière des transactions."""
    if df.empty or 'date' not in df.columns:
        return go.Figure().update_layout(title='Aucune donnée pour la tendance journalière', xaxis={'visible': False}, yaxis={'visible': False})
    # S'assurer que 'date' est bien de type date pour le groupement
    df['date_col'] = pd.to_datetime(df['date'])
    daily_data = df.groupby(df['date_col'].dt.date).size().reset_index(name='transactions')
    fig = px.line(daily_data, x='date_col', y='transactions',
                  title='Tendance Journalière des Transactions',
                  labels={'transactions': 'Nb Transactions', 'date_col': 'Date'})
    fig.update_layout(showlegend=False, height=350, title_x=0.5)
    return fig

def create_recharge_trend_chart(series: pd.Series):
    """Crée un graphique linéaire de la tendance mensuelle des recharges."""
    if series.empty:
        return go.Figure().update_layout(title='Aucune donnée pour la tendance des recharges', xaxis={'visible': False}, yaxis={'visible': False})

    # Assurez-vous que l'index est trié (il devrait l'être si 'month' est YYYY-MM)
    series = series.sort_index()

    fig = px.line(x=series.index, y=series.values,
                  title='Montant Total des Recharges par Mois',
                  labels={'x': 'Mois', 'y': 'Montant Recharge (XOF)'},
                  markers=True)
    fig.update_layout(height=350, title_x=0.5)
    return fig

def create_peak_hours_chart(df_pivot: pd.DataFrame):
    """Crée un graphique (heatmap ou barres empilées) des heures de pointe par direction."""
    if df_pivot.empty:
        return go.Figure().update_layout(title='Aucune donnée pour les heures de pointe', xaxis={'visible': False}, yaxis={'visible': False})

    # Heatmap est souvent plus lisible pour ce type de données
    fig = go.Figure(data=go.Heatmap(
        z=df_pivot.values,
        x=[f"{h}h" for h in df_pivot.columns],
        y=df_pivot.index,
        colorscale='Reds', # Choisir une échelle de couleurs appropriée
        colorbar={'title': 'Nb Transactions'}
    ))
    fig.update_layout(
        title='Heures de Pointe par Direction (Heatmap)',
        xaxis_title='Heure de la journée',
        yaxis_title='Direction',
        height=400 + len(df_pivot.index) * 20, # Ajuster la hauteur selon le nombre de directions
        title_x=0.5
    )
    return fig


# ===============================================
# Fonctions d'affichage de l'UI Streamlit
# ===============================================

@st.cache_resource # Cache l'instance de l'analyzer
def get_analyzer():
    """Retourne une instance de AutorouteAnalyzer."""
    logging.info("Création d'une nouvelle instance de AutorouteAnalyzer.")
    return AutorouteAnalyzer()

@st.cache_data # Cache les données chargées et parsées
def load_and_parse_data(_analyzer: AutorouteAnalyzer, uploaded_pdfs: list, uploaded_excel: BytesIO) -> bool:
    """Charge le fichier Excel et parse les PDFs. Retourne True si succès."""
    if not uploaded_pdfs or not uploaded_excel:
        st.warning("Veuillez charger les fichiers PDF et le fichier Excel.")
        return False

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Sauvegarde temporaire du fichier Excel
            excel_path = Path(temp_dir) / "Liste_cartes_Rapido_temp.xlsx" # Nom unique temporaire
            with open(excel_path, "wb") as f:
                f.write(uploaded_excel.getvalue()) # Utiliser getvalue() pour BytesIO

            # Charger les propriétaires de cartes
            _analyzer.load_card_owners(str(excel_path))
            if not _analyzer.card_owners:
                 st.error("Impossible de charger les propriétaires de cartes. Vérifiez le fichier Excel.")
                 return False

            # Parser les fichiers PDF
            # Utiliser st.spinner pour montrer l'activité
            with st.spinner(f"Traitement de {len(uploaded_pdfs)} fichier(s) PDF..."):
                _analyzer.all_transactions = [] # Réinitialiser avant de parser
                for pdf_file in uploaded_pdfs:
                    try:
                        # Extraire le numéro de compte du nom de fichier (adapter si nécessaire)
                        account_number = Path(pdf_file.name).stem
                        pdf_content = BytesIO(pdf_file.getvalue())
                        _analyzer.parse_pdf_content(pdf_content, account_number)
                    except Exception as e:
                        st.error(f"Erreur lors du traitement du fichier PDF '{pdf_file.name}': {e}")
                        logging.error(f"Erreur traitement PDF {pdf_file.name}", exc_info=True)
                        # Décider si on continue ou arrête en cas d'erreur sur un fichier
                        # return False # Arrêter si une erreur est critique

                _analyzer.finalize_data_loading() # Crée et enrichit le DataFrame final

            if _analyzer.raw_df.empty:
                 st.warning("Aucune transaction n'a pu être extraite des fichiers PDF fournis.")
                 return False

            logging.info("Chargement et parsing des données terminés.")
            return True # Succès

        except Exception as e:
            st.error(f"Une erreur générale est survenue lors du chargement des données: {e}")
            logging.error("Erreur générale chargement données", exc_info=True)
            return False

@st.cache_data # Cache les résultats des analyses
def run_analyses(_analyzer: AutorouteAnalyzer, inactivity_threshold: int):
    """Exécute toutes les analyses sur les données chargées."""
    if not _analyzer.is_data_loaded:
         st.warning("Les données ne sont pas chargées, impossible d'exécuter les analyses.")
         return {}
    logging.info(f"Exécution des analyses avec seuil d'inactivité: {inactivity_threshold} jours.")
    return _analyzer.run_all_analyses(inactivity_threshold)


def display_sidebar(analyzer):
    """Affiche les éléments de la barre latérale et retourne les paramètres."""
    with st.sidebar:
        st.image("https://www.dalmotors.com/web/image/website/1/logo/DAL%20Motors?unique=ce05409", width=150) # Exemple d'ajout de logo

        st.markdown("---")
        # Affichage sécurité et copyright
        st.markdown(
            """
            <div class="security-info">
                <span style="color: #1f77b4; font-size: 1.2em; margin-right: 8px;">🔒</span>
                <span style="color: #666; font-size: 0.9em;">
                Application locale - Traitement en mémoire.
                </span>
            </div>
            """, unsafe_allow_html=True)

        st.header("Paramètres")
        # Heures non ouvrées
        st.subheader("Heures Non Ouvrées")
        start_non_work = st.slider("Heure de début (non ouvrée)", min_value=0, max_value=23, value=analyzer.start_non_work_hour, key="start_non_work")
        end_non_work = st.slider("Heure de fin (non ouvrée)", min_value=0, max_value=23, value=analyzer.end_non_work_hour, key="end_non_work")
        # Mettre à jour l'analyzer si les heures changent
        if start_non_work != analyzer.start_non_work_hour or end_non_work != analyzer.end_non_work_hour:
             analyzer.set_non_working_hours(start_non_work, end_non_work)
             # Note: Streamlit va ré-exécuter le script. Si les données sont cachées,
             # il faudra peut-être invalider le cache ou recalculer l'analyse si les heures changent.
             # Pour l'instant, on met juste à jour l'analyzer. Le recalcul se fera au prochain run_analyses.

        st.markdown("---")
        # Seuil d'inactivité
        st.subheader("Activité des Cartes")
        inactivity_threshold = st.slider(
            "Seuil d'inactivité (jours)",
            min_value=30, max_value=365, value=90, step=15, # Steps plus grands
            help="Nombre de jours sans transaction après lequel une carte est considérée inactive.",
            key="inactivity_slider"
        )

        st.markdown("---")
        st.header("Chargement des Données")
        # Fichiers PDF
        uploaded_pdfs = st.file_uploader(
             "1. Fichiers PDF des relevés Rapido",
             type=['pdf'],
             accept_multiple_files=True,
             help="Chargez un ou plusieurs fichiers PDF contenant les transactions."
        )
        # Fichier Excel
        uploaded_excel = st.file_uploader(
            "2. Fichier Excel des cartes ('Liste cartes Rapido.xlsx')",
            type=['xlsx', 'xls'],
             accept_multiple_files=False,
             help="Chargez le fichier Excel associant les numéros de carte aux directions/utilisateurs (colonne 'N°CARTERAPIDO' et 'USER')."
        )

        st.markdown("---")
        # Copyright Footer in Sidebar
        st.markdown(
             """
             <div style="text-align:center; color:gray; font-size:0.8em; margin-top: 2rem;">
                 Copyright (c) 2025 DAL/GPR - Tous droits réservés
             </div>
             """,
            unsafe_allow_html=True
         )

    return start_non_work, end_non_work, inactivity_threshold, uploaded_pdfs, uploaded_excel


def display_filters(df: pd.DataFrame):
    """Affiche les filtres principaux et retourne les valeurs sélectionnées."""
    if df.empty:
        st.info("Chargez des données pour activer les filtres.")
        return None, None, [], [], None # Retourne des valeurs par défaut

    st.subheader("Filtres Généraux")
    col_f1, col_f2, col_f3 = st.columns([2, 2, 1])

    with col_f1:
        # Filtre Date
        min_date = df['datetime'].min().date()
        max_date = df['datetime'].max().date()
        selected_start_date, selected_end_date = st.date_input(
            "Période d'analyse",
            value=(min_date, max_date), # Sélectionne toute la plage par défaut
            min_value=min_date, max_value=max_date,
            key="date_filter"
        )

    with col_f2:
        # Filtre Direction
        all_directions = sorted(df['direction'].unique().tolist())
        directions_with_all = ["Tous"] + all_directions
        selected_directions = st.multiselect(
            "Directions",
            options=directions_with_all,
            default=["Tous"],
            key="direction_filter"
        )
        if "Tous" in selected_directions:
            selected_directions = all_directions # Utiliser toutes les directions si "Tous" est sélectionné

    with col_f3:
         # Filtres booléens (Weekend, Heures non ouvrées)
         st.markdown("**Conditions Spécifiques**") # Utiliser markdown pour le titre
         filter_we = st.checkbox("Weekend uniquement", key="we_filter")
         filter_hh = st.checkbox("Heures non ouvrées uniquement", key="hh_filter")
         # Convertir les booléens en 'Oui'/'Non'/None pour la fonction de filtrage
         weekend_status = 'Oui' if filter_we else None
         non_working_status = 'Oui' if filter_hh else None


    # Barre de recherche globale
    search_term = st.text_input(
        "Rechercher (N° carte, Montant...)",
        placeholder="Entrez un numéro de carte, montant...",
        key="search_bar"
    )

    return selected_start_date, selected_end_date, selected_directions, [weekend_status, non_working_status], search_term

def display_main_table(df: pd.DataFrame):
    """Affiche le tableau principal des transactions filtrées."""
    st.subheader("Détail des Transactions Filtrées")
    if df.empty:
        st.info("Aucune transaction ne correspond aux filtres sélectionnés.")
        return

    st.write(f"Nombre de transactions affichées : **{len(df)}**")

    # Préparer le DataFrame pour l'affichage (sélection/renommage des colonnes)
    df_display = df.rename(columns={
        'account': 'N° Carte', 'datetime': 'Date & Heure', 'type': 'Type',
        'amount': 'Montant (XOF)', 'direction': 'Direction', 'date': 'Date', 'hour': 'H',
        'day_of_week': 'Jour', 'is_weekend': 'WE', 'is_non_working_hours': 'H. Non Ouvr.'
    })

    # Sélectionner et ordonner les colonnes pour l'affichage
    cols_to_show = ['N° Carte', 'Direction', 'Date & Heure', 'Type', 'Montant (XOF)', 'Jour', 'H', 'WE', 'H. Non Ouvr.']
    df_display_final = df_display[[col for col in cols_to_show if col in df_display.columns]]

    # Afficher avec st.dataframe pour l'interactivité
    st.dataframe(df_display_final, use_container_width=True, height=400) # Hauteur ajustable

    # Bouton de téléchargement pour le tableau filtré
    excel_bytes = to_excel(df_display_final)
    if excel_bytes:
        st.download_button(
            label="📥 Télécharger Tableau Filtré (Excel)",
            data=excel_bytes,
            file_name='transactions_filtrees.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            key='download_filtered_table'
        )

def display_recharge_info(filtered_df: pd.DataFrame, analyses_results: dict):
    """Affiche les informations sur les recharges."""
    st.subheader("Analyse des Recharges")

    # 1. Tableau récapitulatif des recharges par direction (basé sur l'analyse pré-calculée et filtrée)
    recharge_by_direction = analyses_results.get('recharges_par_direction', pd.DataFrame())

    # Filtrer ce tableau récapitulatif en fonction des directions sélectionnées dans l'UI principale
    # (Si on veut que ce tableau reflète aussi le filtre direction de l'UI)
    # directions_filter = st.session_state.get('direction_filter', ["Tous"]) # Récupérer le filtre actuel
    # if "Tous" not in directions_filter and not recharge_by_direction.empty:
    #     recharge_by_direction = recharge_by_direction[recharge_by_direction['Direction'].isin(directions_filter)]

    if not recharge_by_direction.empty:
        st.write("Montant total des recharges par direction (période sélectionnée) :")
        st.dataframe(recharge_by_direction, use_container_width=True)
        excel_bytes_recharge_dir = to_excel(recharge_by_direction)
        if excel_bytes_recharge_dir:
            st.download_button(
                label="📥 Télécharger Recharges par Direction (Excel)",
                data=excel_bytes_recharge_dir,
                file_name='recharges_par_direction.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_recharge_dir'
            )
    else:
        st.info("Aucune transaction de recharge trouvée pour la période/directions sélectionnées.")

    # 2. Tableau détaillé des transactions de recharge (basé sur le DF déjà filtré par l'UI)
    st.write("Détail des transactions de recharge (période/filtres sélectionnés) :")
    df_recharge_detail = filtered_df[filtered_df['type'].str.lower() == 'recharge'].copy()

    if not df_recharge_detail.empty:
         # Préparer pour l'affichage
         df_recharge_display = df_recharge_detail.rename(columns={
             'account': 'N° Carte', 'datetime': 'Date & Heure', 'type': 'Type',
             'amount': 'Montant (XOF)', 'direction': 'Direction', 'date': 'Date', 'hour': 'H',
             'day_of_week': 'Jour', 'is_weekend': 'WE', 'is_non_working_hours': 'H. Non Ouvr.'
         })
         cols_recharge = ['N° Carte', 'Direction', 'Date & Heure', 'Montant (XOF)']
         st.dataframe(df_recharge_display[cols_recharge], use_container_width=True)

         excel_bytes_recharge_detail = to_excel(df_recharge_display[cols_recharge])
         if excel_bytes_recharge_detail:
            st.download_button(
                 label="📥 Télécharger Détail Recharges (Excel)",
                 data=excel_bytes_recharge_detail,
                 file_name='details_recharges.xlsx',
                 mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                 key='download_recharge_detail'
            )
    else:
         st.info("Aucune transaction de recharge détaillée à afficher avec les filtres actuels.")


def display_kpis(df: pd.DataFrame, analyses: dict, inactivity_threshold: int):
    """Affiche les Indicateurs Clés de Performance (KPIs)."""
    st.subheader("Indicateurs Clés (Période Sélectionnée)")

    if df.empty:
        st.warning("Aucune donnée à analyser pour la période sélectionnée.")
        return

    total_transactions = len(df)
    total_amount_transit = df[df['type'].str.lower() == 'transit']['amount'].sum()
    total_recharge_amount = analyses.get('recharges_mensuelles', pd.Series(dtype=float)).sum() # Approximation sur toute la période
    total_promo_amount = df[df['type'].str.lower() == 'promotion']['amount'].sum()
    total_chargements = total_recharge_amount + total_promo_amount

    active_cards_period = df['account'].nunique()

    # Recalculer l'activité spécifiquement pour la période filtrée pour les KPIs
    # Utiliser une fonction helper ou recalculer ici rapidement
    # Note: `_analyze_card_activity` attend le df et le seuil.
    # Attention: Ne pas appeler la méthode privée directement si possible.
    # Solution propre : avoir une méthode publique dans l'analyzer qui prend un df et retourne l'activité.
    # Ici, pour simplifier, on accède aux résultats pré-calculés et on espère qu'ils correspondent à la période,
    # ou on affiche les stats globales si on ne recalcule pas.
    # -> Pour l'instant, utilisons les stats globales d'activité issues de `run_analyses`
    activite_summary = analyses.get('activite_cartes_resume', pd.DataFrame())
    total_cards_all = 0
    active_cards_all = 0
    inactive_cards_all = 0
    pct_active_all = 0
    pct_inactive_all = 0

    if not activite_summary.empty and 'Total' in activite_summary['Direction'].values:
        total_row = activite_summary[activite_summary['Direction'] == 'Total'].iloc[0]
        total_cards_all = total_row['Total_cartes']
        active_cards_all = total_row['Cartes_actives']
        inactive_cards_all = total_row['Cartes_inactives']
        if total_cards_all > 0:
            pct_active_all = (active_cards_all / total_cards_all * 100)
            pct_inactive_all = (inactive_cards_all / total_cards_all * 100)


    # Passages WE/HH sur la période filtrée
    non_working_hours_transactions = df[df['is_non_working_hours'] & ~df['is_weekend']]
    weekend_transactions = df[df['is_weekend']]
    montant_hh = non_working_hours_transactions[non_working_hours_transactions['type'].str.lower() == 'transit']['amount'].sum()
    montant_we = weekend_transactions[weekend_transactions['type'].str.lower() == 'transit']['amount'].sum()
    pct_non_work = (len(non_working_hours_transactions) / total_transactions * 100) if total_transactions else 0
    pct_we = (len(weekend_transactions) / total_transactions * 100) if total_transactions else 0

    # Affichage des KPIs
    col_k1, col_k2, col_k3 = st.columns(3)
    with col_k1:
        st.metric("Transactions (période)", f"{total_transactions:,}")
        st.metric("Montant Transit (période)", f"{total_amount_transit:,.0f} XOF")
        # st.metric("Montant Chargements (Total)", f"{total_chargements:,.0f} XOF") # Peut être trompeur si basé sur analyse globale
    with col_k2:
        st.metric("Cartes Actives (période)", f"{active_cards_period}")
        st.metric("Passages WE (période)", f"{pct_we:.1f}%", f"{montant_we:,.0f} XOF")
        st.metric("Passages H. Non Ouvr. (période, L-V)", f"{pct_non_work:.1f}%", f"{montant_hh:,.0f} XOF")
    with col_k3:
        st.metric(f"Total Cartes Connues", f"{total_cards_all}")
        st.metric(f"Actives ({inactivity_threshold}j)", f"{active_cards_all}", f"{pct_active_all:.1f}%")
        st.metric(f"Inactives ({inactivity_threshold}j)", f"{inactive_cards_all}", f"{pct_inactive_all:.1f}%")


def display_visualizations(df: pd.DataFrame, analyses_results: dict):
    """Affiche les graphiques principaux."""
    st.subheader("Visualisations (Période Sélectionnée)")

    if df.empty:
        st.warning("Aucune donnée à visualiser pour la période sélectionnée.")
        return

    # Créer une copie pour éviter les SettingWithCopyWarning si on modifie df_visu
    df_visu = df.copy()

    # Filtrage supplémentaire pour les visualisations (Mois/Jour spécifique)
    # Note: Ce filtrage s'applique *après* les filtres généraux
    col_v1, col_v2 = st.columns(2)
    with col_v1:
         available_months = ["Tous"] + sorted(df_visu['month'].unique().tolist())
         selected_month = st.selectbox(
             "Affiner par mois",
             options=available_months,
             index=0, # Default to "Tous"
             key="visu_month_filter"
         )
         if selected_month != "Tous":
             df_visu = df_visu[df_visu['month'] == selected_month]

    with col_v2:
        available_days = ["Tous"]
        if not df_visu.empty:
             # Convertir en date avant de trier
             unique_dates = pd.to_datetime(df_visu['date']).dt.date.unique()
             available_days.extend(sorted(unique_dates))

        selected_day = st.selectbox(
             "Affiner par jour",
             options=available_days,
             index=0, # Default to "Tous"
             key="visu_day_filter"
         )
        if selected_day != "Tous":
             df_visu = df_visu[pd.to_datetime(df_visu['date']).dt.date == selected_day]


    # Afficher les graphiques basés sur df_visu (doublement filtré)
    if df_visu.empty:
        st.info("Aucune donnée pour les filtres de visualisation sélectionnés.")
        return

    st.markdown("---") # Séparateur visuel

    # Disposition des graphiques
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(create_monthly_trend_chart(df_visu), use_container_width=True)
        st.plotly_chart(create_weekday_bar_chart(df_visu), use_container_width=True)
        # Afficher la tendance journalière si un mois est sélectionné mais pas un jour précis
        if selected_month != "Tous" and selected_day == "Tous":
            st.plotly_chart(create_daily_trend_chart(df_visu), use_container_width=True)

    with col2:
        st.plotly_chart(create_direction_pie_chart(df_visu), use_container_width=True)
        st.plotly_chart(create_hourly_heatmap(df_visu), use_container_width=True)
        # Afficher la tendance des recharges (basée sur les données globales filtrées par mois/jour)
        recharges_trend_data = df_visu[df_visu['type'].str.lower() == 'recharge'].groupby('month')['amount'].sum()
        st.plotly_chart(create_recharge_trend_chart(recharges_trend_data), use_container_width=True)


    # Afficher l'analyse des heures de pointe (basée sur df_visu)
    st.markdown("---")
    st.subheader("Analyse des Heures de Pointe (période/filtres affinés)")
    peak_hours_data = df_visu.groupby(['direction', 'hour']).size().unstack(fill_value=0)
    # S'assurer que toutes les heures sont présentes
    all_hours = list(range(24))
    peak_hours_data = peak_hours_data.reindex(columns=all_hours, fill_value=0)

    if not peak_hours_data.empty:
        # Choix entre Table et Graphique
        view_type = st.radio("Vue Heures de Pointe:", ["Tableau", "Graphique (Heatmap)"], index=1, key="peak_hours_view", horizontal=True)
        if view_type == "Tableau":
            st.dataframe(peak_hours_data, use_container_width=True)
        else:
            st.plotly_chart(create_peak_hours_chart(peak_hours_data), use_container_width=True)

        excel_bytes_peak = to_excel(peak_hours_data.reset_index()) # Reset index pour export
        if excel_bytes_peak:
             st.download_button(
                 label="📥 Télécharger Heures de Pointe (Excel)",
                 data=excel_bytes_peak,
                 file_name='heures_pointe_par_direction.xlsx',
                 mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                 key='download_peak_hours'
             )
    else:
        st.info("Aucune donnée de transaction pour analyser les heures de pointe avec les filtres actuels.")


def display_card_analysis(df: pd.DataFrame, analyzer: AutorouteAnalyzer, inactivity_threshold: int):
    """Affiche l'analyse spécifique à une carte recherchée."""
    st.header("🔍 Analyse par Carte Individuelle")
    card_search_term = st.text_input(
        "Rechercher une carte par son numéro",
        key="card_search_input",
        placeholder="Entrez le numéro exact de la carte"
    )

    if card_search_term:
        card_search_term = card_search_term.strip() # Nettoyer l'input
        # Filtrer le DataFrame global (non filtré par date/direction ici) pour cette carte
        card_df = df[df['account'] == card_search_term]

        if not card_df.empty:
            st.subheader(f"Analyse pour la carte : {card_search_term}")

            # 1. Informations générales et statut d'activité
            activite_details = analyzer.analysis_results.get('activite_cartes_details', pd.DataFrame())
            card_activity_info = activite_details[activite_details['Numéro_carte_Rapido'] == card_search_term]

            if not card_activity_info.empty:
                info = card_activity_info.iloc[0]
                status_class = "custom-status-active" if info['Statut'] == "Active" else "custom-status-inactive"
                st.markdown(f"""
                    <div class='custom-info-box'>
                        Direction: **{info['Direction']}**<br>
                        Statut (seuil {inactivity_threshold}j): <span class='{status_class}'>{info['Statut']}</span><br>
                        Dernière transaction: {info['Dernière_transaction'] if pd.notna(info['Dernière_transaction']) else 'N/A'}<br>
                        Jours depuis dernière transaction: {info['Jours_depuis_derniere_transaction'] if pd.notna(info['Jours_depuis_derniere_transaction']) else 'N/A'}
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.warning(f"Statut d'activité non trouvé pour la carte {card_search_term} (peut-être inconnue?).")

            # 2. Statistiques clés pour cette carte (sur la période globale des données chargées)
            total_trans = len(card_df)
            total_transit_amount = card_df[card_df['type'].str.lower() == 'transit']['amount'].sum()
            total_recharge_amount = card_df[card_df['type'].str.lower() == 'recharge']['amount'].sum()

            st.metric("Nb Total Transactions (carte)", f"{total_trans}")
            st.metric("Montant Total Transit (carte)", f"{total_transit_amount:,.0f} XOF")
            st.metric("Montant Total Recharges (carte)", f"{total_recharge_amount:,.0f} XOF")


            # 3. Tableau des transactions de la carte (WE ou HH)
            st.subheader("Transactions Weekend ou Hors Heures Ouvrées (Carte)")
            card_df_we_hh = card_df[card_df['is_weekend'] | card_df['is_non_working_hours']]

            if not card_df_we_hh.empty:
                # Préparer pour affichage
                card_df_display = card_df_we_hh.rename(columns={
                     'account': 'N° Carte', 'datetime': 'Date & Heure', 'type': 'Type',
                     'amount': 'Montant (XOF)', 'direction': 'Direction', 'date': 'Date', 'hour': 'H',
                     'day_of_week': 'Jour', 'is_weekend': 'WE', 'is_non_working_hours': 'H. Non Ouvr.'
                 })
                cols_card = ['Date & Heure', 'Type', 'Montant (XOF)', 'Jour', 'H', 'WE', 'H. Non Ouvr.']
                st.dataframe(card_df_display[cols_card], use_container_width=True)

                excel_bytes_card = to_excel(card_df_display[cols_card])
                if excel_bytes_card:
                     st.download_button(
                         label=f"📥 Télécharger Transactions WE/HH Carte {card_search_term} (Excel)",
                         data=excel_bytes_card,
                         file_name=f'transactions_we_hh_carte_{card_search_term}.xlsx',
                         mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         key=f'download_card_{card_search_term}'
                     )
            else:
                 st.info("Cette carte n'a pas de transactions enregistrées pendant le week-end ou en dehors des heures ouvrées sur la période chargée.")

        else:
            st.warning(f"Aucune transaction trouvée pour le numéro de carte '{card_search_term}'. Vérifiez le numéro ou si la carte existe dans les relevés.")

def display_key_analyses(analyses: dict):
    """Affiche les tableaux d'analyses clés."""
    st.header("📋 Analyses Clés (Basées sur Toutes les Données Chargées)")
    st.caption("Ces tableaux présentent les analyses sur l'ensemble des données chargées, indépendamment des filtres de date/direction ci-dessus.")

    if not analyses:
        st.warning("Les analyses n'ont pas pu être exécutées. Chargez des données valides.")
        return

    # Utiliser des expanders pour organiser les analyses
    with st.expander("1. Passages Mensuels par Direction", expanded=False):
        df_pass_mens = analyses.get('passages_mensuels_par_direction', pd.DataFrame())
        if not df_pass_mens.empty:
            st.dataframe(df_pass_mens, use_container_width=True)
            excel_bytes = to_excel(df_pass_mens)
            if excel_bytes:
                st.download_button("📥 Télécharger (Excel)", excel_bytes, "passages_mensuels_direction.xlsx", key="dl_pass_mens")
        else:
            st.info("Données non disponibles.")

    with st.expander("2. Nombre de Passages & Montants par Carte/Direction", expanded=False):
        df_nb_pass = analyses.get('nb_passages_par_carte_direction', pd.DataFrame())
        if not df_nb_pass.empty:
            # Ajout d'une recherche simple pour ce tableau
            search_passages = st.text_input("Rechercher (N° carte / Direction)", key="search_nb_pass")
            df_to_show = df_nb_pass
            if search_passages:
                df_to_show = df_nb_pass[
                    df_nb_pass['Numéro carte Rapido'].astype(str).str.contains(search_passages, case=False, na=False) |
                    df_nb_pass['Direction'].astype(str).str.contains(search_passages, case=False, na=False)
                ]
            st.dataframe(df_to_show, use_container_width=True)
            excel_bytes = to_excel(df_nb_pass) # Exporter tout le tableau
            if excel_bytes:
                st.download_button("📥 Télécharger (Excel)", excel_bytes, "passages_montants_carte_direction.xlsx", key="dl_nb_pass")
        else:
            st.info("Données non disponibles.")

    with st.expander("3. Nombre de Cartes Actives par Direction (sur période chargée)", expanded=False):
        df_nb_cartes = analyses.get('nb_cartes_par_direction', pd.DataFrame())
        if not df_nb_cartes.empty:
            st.dataframe(df_nb_cartes, use_container_width=True)
            excel_bytes = to_excel(df_nb_cartes)
            if excel_bytes:
                st.download_button("📥 Télécharger (Excel)", excel_bytes, "nb_cartes_actives_direction.xlsx", key="dl_nb_cartes")
        else:
            st.info("Données non disponibles.")

    with st.expander("4. Cartes avec Consommation Week-end / Hors Heures Ouvrées (Transit)", expanded=False):
        df_wehh = analyses.get('cartes_weekend_heures_non_ouvrees', pd.DataFrame())
        if not df_wehh.empty:
            search_wehh = st.text_input("Rechercher (N° carte / Direction)", key="search_wehh")
            df_to_show = df_wehh
            if search_wehh:
                 df_to_show = df_wehh[
                     df_wehh['Numéro carte Rapido'].astype(str).str.contains(search_wehh, case=False, na=False) |
                     df_wehh['Direction'].astype(str).str.contains(search_wehh, case=False, na=False)
                 ]
            st.dataframe(df_to_show, use_container_width=True)
            excel_bytes = to_excel(df_wehh)
            if excel_bytes:
                st.download_button("📥 Télécharger (Excel)", excel_bytes, "cartes_conso_we_hh.xlsx", key="dl_wehh")
        else:
            st.info("Données non disponibles ou aucune consommation de ce type trouvée.")

    with st.expander("5. Analyse d'Activité des Cartes (Globale)", expanded=True): # Ouvrir par défaut
        df_act_sum = analyses.get('activite_cartes_resume', pd.DataFrame())
        df_act_det = analyses.get('activite_cartes_details', pd.DataFrame())

        st.subheader("Résumé par Direction")
        if not df_act_sum.empty:
            st.dataframe(df_act_sum, use_container_width=True)
            excel_bytes = to_excel(df_act_sum)
            if excel_bytes:
                st.download_button("📥 Télécharger Résumé (Excel)", excel_bytes, "resume_activite_cartes.xlsx", key="dl_act_sum")
        else:
            st.info("Résumé non disponible.")

        st.subheader("Détails par Carte")
        if not df_act_det.empty:
            search_act = st.text_input("Rechercher (N° carte / Direction / Statut)", key="search_act_det")
            df_to_show = df_act_det
            if search_act:
                 search_act_lower = search_act.lower()
                 df_to_show = df_act_det[
                     df_act_det['Numéro_carte_Rapido'].astype(str).str.contains(search_act, case=False, na=False) |
                     df_act_det['Direction'].astype(str).str.contains(search_act, case=False, na=False) |
                     df_act_det['Statut'].astype(str).str.lower().str.contains(search_act_lower, na=False)
                 ]
            st.dataframe(df_to_show, use_container_width=True)
            excel_bytes = to_excel(df_act_det)
            if excel_bytes:
                st.download_button("📥 Télécharger Détails (Excel)", excel_bytes, "details_activite_cartes.xlsx", key="dl_act_det")
        else:
            st.info("Détails non disponibles.")


# ===============================================
# Fonctions principales de l'application
# ===============================================

def module_rapido(analyzer):
    """Affiche l'interface du module d'analyse Rapido."""
    st.title("📊 Analyseur de Relevés Rapido")

    # --- Sidebar et chargement des données ---
    start_h, end_h, inactivity_th, pdfs, excel = display_sidebar(analyzer)

    data_loaded = False
    analysis_results = {}
    df_raw = pd.DataFrame()

    # Logique de chargement et d'analyse déclenchée SI les fichiers sont présents
    if pdfs and excel:
        # Utiliser l'état de session pour éviter rechargement si fichiers n'ont pas changé ?
        # Pour l'instant, on recharge si les objets fichiers sont là.
        # Le cache @st.cache_data gérera l'exécution réelle.
        data_loaded = load_and_parse_data(analyzer, pdfs, excel)

        if data_loaded and analyzer.is_data_loaded:
             df_raw = analyzer.raw_df
             # Exécuter les analyses après chargement (sera caché aussi)
             analysis_results = run_analyses(analyzer, inactivity_th)
        else:
             st.error("Le chargement ou le parsing des données a échoué. Vérifiez les fichiers et les logs.")
             # Assurer que df_raw et analysis_results sont vides si échec
             df_raw = pd.DataFrame()
             analysis_results = {}
    else:
        st.info("ℹ️ Veuillez charger les fichiers PDF et Excel requis dans la barre latérale pour démarrer l'analyse.")

    # Si les données sont chargées, afficher le reste de l'interface
    if data_loaded and not df_raw.empty:

        # --- Filtres principaux ---
        start_d, end_d, dirs, we_hh_filters, search = display_filters(df_raw)
        weekend_filter, hh_filter = we_hh_filters # Dépaqueter les filtres booléens

        # --- Filtrage des données pour affichage ---
        # Le filtrage se base maintenant sur les sélections de l'UI
        filtered_data = analyzer.get_filtered_data(
             start_date=start_d,
             end_date=end_d,
             directions=dirs,
             weekend_filter=weekend_filter, # Utilise 'Oui' ou None
             non_working_hours_filter=hh_filter, # Utilise 'Oui' ou None
             search_term=search
        )

        # --- Bouton d'Export Général ---
        st.markdown("---")
        st.subheader("Exporter l'Analyse Complète")
        # Utiliser une colonne pour mieux placer le bouton
        col_export1, _ = st.columns([1, 3])
        with col_export1:
            export_filename = f"Analyse_Rapido_{start_d.strftime('%Y%m%d')}_au_{end_d.strftime('%Y%m%d')}.xlsx"
            # Utiliser un NamedTemporaryFile pour l'export
            # Le bouton génère l'export à la volée quand on clique
            if st.button("📊 Générer l'Export Excel (Période Filtrée)"):
                 with st.spinner("Préparation de l'export Excel..."):
                      try:
                           # Utiliser NamedTemporaryFile pour obtenir un chemin de fichier
                           with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmpfile:
                                analyzer.export_analyses_to_excel(
                                    tmpfile.name,
                                    start_date=start_d,
                                    end_date=end_d,
                                    inactivity_threshold=inactivity_th
                                )
                                # Lire le contenu du fichier temporaire pour le download button
                                tmpfile.seek(0)
                                excel_export_data = tmpfile.read()

                           # Stocker les données dans session_state pour le download button
                           st.session_state['excel_export_data'] = excel_export_data
                           st.session_state['excel_export_filename'] = export_filename

                      except Exception as e:
                           st.error(f"Erreur lors de la génération de l'export : {e}")
                           logging.error("Erreur génération export général", exc_info=True)
                           st.session_state['excel_export_data'] = None # Réinitialiser en cas d'erreur

        # Afficher le bouton de téléchargement si les données sont prêtes en session_state
        if 'excel_export_data' in st.session_state and st.session_state['excel_export_data']:
              st.download_button(
                   label="📥 Télécharger l'Analyse Complète",
                   data=st.session_state['excel_export_data'],
                   file_name=st.session_state['excel_export_filename'],
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                   key="download_main_export",
                   # Supprimer les données de session_state après le téléchargement (optionnel)
                   # on_click=lambda: st.session_state.pop('excel_export_data', None)
              )


        st.markdown("---")

        # --- Affichage Tableau Principal ---
        display_main_table(filtered_data)

        st.markdown("---")

        # --- Affichage Infos Recharges ---
        # Passe le DF déjà filtré par l'UI et les analyses globales
        display_recharge_info(filtered_data, analysis_results)

        st.markdown("---")

        # --- Affichage KPIs ---
        display_kpis(filtered_data, analysis_results, inactivity_th)

        st.markdown("---")

        # --- Affichage Visualisations ---
        display_visualizations(filtered_data, analysis_results)

        st.markdown("---")

        # --- Affichage Analyse par Carte ---
        # Passe le DF brut global (df_raw) car l'analyse par carte n'est pas liée aux filtres généraux
        display_card_analysis(df_raw, analyzer, inactivity_th)

        st.markdown("---")

        # --- Affichage Analyses Clés ---
        # Passe les résultats d'analyse globaux
        display_key_analyses(analysis_results)

    # Afficher le footer à la fin de la page principale
    st.markdown("<div class='footer'>Copyright (c) 2025 DAL/GPR - Tous droits réservés</div>", unsafe_allow_html=True)


def module_accueil():
    """Affiche la page d'accueil de l'application."""
    col_logo, col_title = st.columns([1, 5])
    with col_logo:
        st.image("https://www.dalmotors.com/web/image/website/1/logo/DAL%20Motors?unique=ce05409", width=100)
    with col_title:
        st.title("Portail d'Applications d'Analyse")

    st.markdown("---")
    st.markdown(
        """
        Bienvenue sur le portail d'analyse de données de DAL/GPR.
        Sélectionnez un module ci-dessous pour commencer.

        <div class="security-info" style="justify-content: flex-start;">
            <span style="color: #1f77b4; font-size: 1.2em; margin-right: 8px;">🔒</span>
            <span style="color: #666; font-size: 0.9em;">
            Application locale sécurisée. Traitement des données en mémoire uniquement.
            </span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("## Modules Disponibles")

    modules = {
        "Analyse des passages Rapido": {
            "description": "Analyser les relevés de transactions des cartes péage Rapido.",
            "icon": "🚗",
            "key": "rapido"
        },
        "Analyse Carburant": {
            "description": "Analyser les transactions des cartes carburant.",
            "icon": "⛽",
            "key": "carburant"
        }
    }

    # Afficher les modules sous forme de cartes ou de boutons
    for module_name, info in modules.items():
        st.subheader(f"{info['icon']} {module_name}")
        st.write(info['description'])
        if st.button(f"Lancer {module_name}", key=f"btn_{info['key']}", use_container_width=False): # Bouton plus petit
            st.session_state.module = module_name
            st.rerun() # Forcer la réexécution pour changer de module
        st.markdown("---")

    # Footer de la page d'accueil
    st.markdown("<div class='footer'>Copyright (c) 2025 DAL/GPR - Tous droits réservés</div>", unsafe_allow_html=True)


def main():
    """Fonction principale qui gère la navigation entre les modules."""
    # Initialiser l'analyzer une seule fois et le mettre en cache via get_analyzer()
    analyzer = get_analyzer()

    # Logique de navigation basée sur st.session_state
    if 'module' not in st.session_state or st.session_state.module is None:
        module_accueil()
    else:
        # Afficher le bouton de retour dans la barre latérale si on est dans un module
        if st.sidebar.button("← Retour à l'Accueil", key="back_home"):
            st.session_state.module = None
            # Nettoyer potentiellement d'autres états de session spécifiques au module si nécessaire
            st.rerun()

        # Exécuter le module sélectionné
        if st.session_state.module == "Analyse des passages Rapido":
            module_rapido(analyzer)
        elif st.session_state.module == "Analyse Carburant":
            module_carburant_main() # Call the imported main function from carburant.py
        else:
            # Si l'état est invalide, retourner à l'accueil
            st.warning("Module non reconnu, retour à l'accueil.")
            st.session_state.module = None
            st.rerun()

if __name__ == "__main__":
    main()

