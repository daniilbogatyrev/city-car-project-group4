import pandas as pd
import os


class CityCarDataHandler:
    """Klasse zum Laden und Vorbereiten der CityCar Daten."""

    def __init__(self, data_folder='data'):
        self.data_folder = data_folder
        self.df_downloads = None
        self.df_signups = None
        self.df_requests = None
        self.df_transactions = None
        self.df_reviews = None
        self.df_funnel = None

    def load_data(self):
        """Lädt alle CSV Dateien und wandelt Zeitspalten in Datetime-Format um."""
        try:
            print("Lade Daten...")
            self.df_downloads = pd.read_csv(os.path.join(self.data_folder, 'app_downloads.csv'))
            self.df_signups = pd.read_csv(os.path.join(self.data_folder, 'signups.csv'))
            self.df_requests = pd.read_csv(os.path.join(self.data_folder, 'ride_requests.csv'))
            self.df_transactions = pd.read_csv(os.path.join(self.data_folder, 'transactions.csv'))
            self.df_reviews = pd.read_csv(os.path.join(self.data_folder, 'reviews.csv'))

            self.df_requests['pickup_ts'] = pd.to_datetime(self.df_requests['pickup_ts'])
            self.df_requests['dropoff_ts'] = pd.to_datetime(self.df_requests['dropoff_ts'])
            self.df_requests['request_ts'] = pd.to_datetime(self.df_requests['request_ts'])
            self.df_requests['accept_ts'] = pd.to_datetime(self.df_requests['accept_ts'])
            self.df_requests['cancel_ts'] = pd.to_datetime(self.df_requests['cancel_ts'])

            print("Daten erfolgreich geladen und Zeiten konvertiert.")
        except Exception as e:
            print(f"Fehler beim Laden: {e}")

    def get_raw_tables(self):
        """Gibt alle einzelnen Tabellen in einem Dictionary zurück."""
        if self.df_downloads is None:
            self.load_data()

        return {
            'Downloads': self.df_downloads,
            'Signups': self.df_signups,
            'Requests': self.df_requests,
            'Transactions': self.df_transactions,
            'Reviews': self.df_reviews
        }

    def merge_all_data(self):
        """Verbindet alle Tabellen mittels LEFT JOINS zu einem Funnel-DataFrame."""
        if self.df_downloads is None:
            self.load_data()

        print("Starte Merging der Tabellen...")

        self.df_funnel = pd.merge(
            self.df_downloads,
            self.df_signups,
            how='left',
            left_on='app_download_key',
            right_on='session_id'
        )

        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_requests,
            how='left',
            on='user_id'
        )

        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_transactions,
            how='left',
            on='ride_id'
        )

        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_reviews,
            how='left',
            on='ride_id'
        )

        if 'driver_id_x' in self.df_funnel.columns:
            self.df_funnel.rename(columns={'driver_id_x': 'driver_id'}, inplace=True)

        if 'user_id_x' in self.df_funnel.columns:
            self.df_funnel.rename(columns={'user_id_x': 'user_id'}, inplace=True)

        print(f"Merging abgeschlossen. Master-Table Größe: {self.df_funnel.shape}")
        return self.df_funnel

    def analyze_ride_duration_quality(self):
        """Analysiert die Fahrtdauer auf Ausreißer."""
        if self.df_requests is None:
            self.load_data()

        durations = (self.df_requests['dropoff_ts'] - self.df_requests['pickup_ts']).dt.total_seconds() / 60
        stats_report = durations.describe()
        long_rides = durations[durations > 300].count()
        negative_rides = durations[durations < 0].count()

        return stats_report, long_rides, negative_rides

    def get_warmup_stats(self):
        """Beantwortet die Warm-up Fragen aus der Aufgabe."""
        if self.df_requests is None:
            self.load_data()

        stats = {
            '1_downloads': len(self.df_downloads),
            '2_signups': len(self.df_signups),
            '3_rides_requested': len(self.df_requests),
            '4_rides_completed': self.df_requests['dropoff_ts'].count(),
            '5_unique_users_requesting': self.df_requests['user_id'].nunique(),
            '6_avg_duration_minutes': round(
                ((self.df_requests['dropoff_ts'] - self.df_requests['pickup_ts']).dt.total_seconds() / 60).mean(), 2
            ),
            '7_rides_accepted': self.df_requests['accept_ts'].count(),
            '8_total_revenue': self.df_transactions['purchase_amount_usd'].sum(),
            '9_platform_counts': self.df_downloads['platform'].value_counts().to_dict()
        }

        return stats

    def calculate_funnel_steps(self):
        """Berechnet die Anzahl der Unique Users für jede Funnel-Stufe."""
        if self.df_funnel is None:
            self.merge_all_data()

        step_1_downloads = self.df_funnel['app_download_key'].nunique()
        step_2_signups = self.df_funnel['user_id'].nunique()
        users_requested = self.df_funnel[self.df_funnel['request_ts'].notna()]['user_id'].nunique()
        users_accepted = self.df_funnel[self.df_funnel['accept_ts'].notna()]['user_id'].nunique()
        users_completed = self.df_funnel[self.df_funnel['dropoff_ts'].notna()]['user_id'].nunique()
        users_paid = self.df_funnel[self.df_funnel['charge_status'] == 'Approved']['user_id'].nunique()
        users_reviewed = self.df_funnel[self.df_funnel['review_id'].notna()]['user_id'].nunique()

        return {
            'steps': ['Downloads', 'Signups', 'Requests', 'Accepted', 'Completed', 'Payment', 'Reviews'],
            'counts': [step_1_downloads, step_2_signups, users_requested, users_accepted, 
                       users_completed, users_paid, users_reviewed]
        }

    def get_patience_metrics(self):


        if self.df_requests is None: self.load_data()


# 1. PHASE SUCHE (Request -> Accept)

# Realität: Wie lange dauert es im Median, bis akzeptiert wird?

        search_reality = (self.df_requests[self.df_requests['accept_ts'].notna()]['accept_ts'] -


        self.df_requests[self.df_requests['accept_ts'].notna()]['request_ts']).dt.total_seconds().median() / 60

# Geduld: Wie lange warten Nutzer, die dann abbrechen (ohne Zusage)?


        search_patience = (self.df_requests[(self.df_requests['cancel_ts'].notna()) & (self.df_requests['accept_ts'].isna())]['cancel_ts'] -
        self.df_requests[(self.df_requests['cancel_ts'].notna()) & (self.df_requests['accept_ts'].isna())]['request_ts']).dt.total_seconds().median() / 60

# 2. PHASE ABHOLUNG (Accept -> Pickup)

# Realität: Wie lange braucht der Fahrer zum Kunden?
        pickup_reality = (self.df_requests[self.df_requests['pickup_ts'].notna()]['pickup_ts'] - self.df_requests[self.df_requests['pickup_ts'].notna()]['accept_ts']).dt.total_seconds().median() / 60

# Geduld: Wie lange warten Nutzer nach der Zusage, bevor sie DOCH NOCH stornieren?


        pickup_patience = (self.df_requests[(self.df_requests['cancel_ts'].notna()) & (self.df_requests['accept_ts'].notna())]['cancel_ts'] - self.df_requests[(self.df_requests['cancel_ts'].notna()) & (self.df_requests['accept_ts'].notna())]['accept_ts']).dt.total_seconds().median() / 60

        return {

'Phasen': ['1. Fahrersuche', '1. Fahrersuche', '2. Abholung', '2. Abholung'],

'Typ': ['Realität (Wartezeit)', 'Geduld (Limit)', 'Realität (Anfahrt)', 'Geduld (Limit)'],

'Minuten': [search_reality, search_patience, pickup_reality, pickup_patience],

'Farbe': ['#3498db', '#95a5a6', '#e74c3c', '#95a5a6'] # Blau, Grau, Rot (Problem), Grau

} 
    


    def analyze_dropoff_gap(self):
        """Untersucht, warum Fahrten akzeptiert, aber nicht abgeschlossen werden."""
        if self.df_requests is None:
            self.load_data()

        print("\n" + "-" * 40)
        print("ANALYSE: WARUM DER ABBRUCH NACH 'ACCEPTED'?")
        print("-" * 40)

        problem_rides = self.df_requests[
            (self.df_requests['accept_ts'].notna()) &
            (self.df_requests['dropoff_ts'].isna())
        ]

        count_problems = len(problem_rides)
        print(f"1. Fahrten akzeptiert aber nicht beendet: {count_problems}")

        cancelled_count = problem_rides['cancel_ts'].notna().sum()
        print(f"2. Davon offiziell storniert (cancel_ts): {cancelled_count}")

        if count_problems > 0:
            quote = (cancelled_count / count_problems) * 100
            print(f"-> Das sind {quote:.1f}% der Fälle!")
        else:
            print("-> Keine Fälle gefunden.")

        print("-" * 40)

    def analyze_cancellation_reasons(self):
        """Analysiert Wartezeiten als Grund für Stornierungen."""
        if self.df_requests is None:
            self.load_data()

        print("\n" + "-" * 50)
        print("      DEEP DIVE: WARTEZEITEN & STORNIERUNGEN      ")
        print("-" * 50)

        completed_rides = self.df_requests[self.df_requests['dropoff_ts'].notna()].copy()
        completed_rides['wait_time'] = (
            completed_rides['pickup_ts'] - completed_rides['accept_ts']
        ).dt.total_seconds() / 60
        avg_wait_completed = completed_rides['wait_time'].mean()

        print(f"Ø Wartezeit bei erfolgreichen Fahrten:  {avg_wait_completed:.2f} Minuten")

        cancelled_rides = self.df_requests[
            (self.df_requests['accept_ts'].notna()) &
            (self.df_requests['dropoff_ts'].isna()) &
            (self.df_requests['cancel_ts'].notna())
        ].copy()

        cancelled_rides['patience_time'] = (
            cancelled_rides['cancel_ts'] - cancelled_rides['accept_ts']
        ).dt.total_seconds() / 60
        avg_wait_cancelled = cancelled_rides['patience_time'].mean()

        print(f"Ø Wartezeit vor Stornierung (Geduld): {avg_wait_cancelled:.2f} Minuten")

        long_waiters = cancelled_rides[cancelled_rides['patience_time'] > 10]
        print(f"Anzahl Stornierer mit Wartezeit > 10 Min: {len(long_waiters)} "
              f"({len(long_waiters) / len(cancelled_rides) * 100:.1f}%)")

        cancelled_rides['hour'] = cancelled_rides['cancel_ts'].dt.hour
        top_hours = cancelled_rides['hour'].value_counts().head(3)
        print("\nTop 3 Stunden mit den meisten Stornierungen:")
        print(top_hours)

        return avg_wait_completed, avg_wait_cancelled

    def get_platform_metrics(self):
        """Analysiert den Funnel getrennt nach Plattform (ios, android, web)."""
        if self.df_funnel is None:
            self.merge_all_data()

        platform_stats = self.df_funnel.groupby('platform').agg({
            'app_download_key': 'nunique',
            'dropoff_ts': lambda x: x.notna().sum()
        }).reset_index()

        platform_stats.columns = ['Platform', 'Downloads', 'Completed_Rides']
        platform_stats['Conversion_Rate'] = (
            platform_stats['Completed_Rides'] / platform_stats['Downloads']
        ) * 100

        return platform_stats

    def get_funnel_by_age(self):
        """Berechnet den Funnel getrennt nach Altersgruppen."""
        if self.df_funnel is None:
            self.merge_all_data()

        df_age = self.df_funnel[self.df_funnel['age_range'].notna()]
        results = []

        for group in df_age['age_range'].unique():
            group_data = df_age[df_age['age_range'] == group]

            results.append({
                'Age_Group': group,
                '1_Signups': group_data['user_id'].nunique(),
                '2_Requests': group_data[group_data['request_ts'].notna()]['user_id'].nunique(),
                '3_Completed': group_data[group_data['dropoff_ts'].notna()]['user_id'].nunique(),
                '4_Reviews': group_data[group_data['review_id'].notna()]['user_id'].nunique()
            })

        df_results = pd.DataFrame(results)
        return df_results.sort_values('Age_Group')

    def analyze_surge_demand(self):
        """Analysiert die Nachfrage nach Tageszeit für Surge Pricing."""
        if self.df_requests is None:
            self.load_data()

        self.df_requests['request_ts'] = pd.to_datetime(self.df_requests['request_ts'])
        df_temp = self.df_requests[['request_ts']].copy()
        df_temp['hour'] = df_temp['request_ts'].dt.hour

        return df_temp['hour'].value_counts().sort_index()
    

    