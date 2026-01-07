import pandas as pd
import os


class CityCarDataHandler:
    """
    Klasse zum Laden und Vorbereiten der CityCar Daten.
    """

    def __init__(self, data_folder='data'):
        self.data_folder = data_folder
        self.df_downloads = None
        self.df_signups = None
        self.df_requests = None
        self.df_transactions = None
        self.df_reviews = None
        self.df_funnel = None

    def load_data(self):
        """Lädt alle CSV Dateien und wandelt ALLE Zeitspalten in echtes Datetime-Format um."""
        try:
            print("Lade Daten...")
            self.df_downloads = pd.read_csv(os.path.join(self.data_folder, 'app_downloads.csv'))
            self.df_signups = pd.read_csv(os.path.join(self.data_folder, 'signups.csv'))
            self.df_requests = pd.read_csv(os.path.join(self.data_folder, 'ride_requests.csv'))
            self.df_transactions = pd.read_csv(os.path.join(self.data_folder, 'transactions.csv'))
            self.df_reviews = pd.read_csv(os.path.join(self.data_folder, 'reviews.csv'))

            # WICHTIG: Zeitspalten konvertieren
            # Bisher hatten wir nur diese drei:
            self.df_requests['pickup_ts'] = pd.to_datetime(self.df_requests['pickup_ts'])
            self.df_requests['dropoff_ts'] = pd.to_datetime(self.df_requests['dropoff_ts'])
            self.df_requests['request_ts'] = pd.to_datetime(self.df_requests['request_ts'])

            # NEU HINZUFÜGEN (Damit der neue Code funktioniert):
            self.df_requests['accept_ts'] = pd.to_datetime(self.df_requests['accept_ts'])
            self.df_requests['cancel_ts'] = pd.to_datetime(self.df_requests['cancel_ts'])

            print("Daten erfolgreich geladen und ALLE Zeiten konvertiert.")
        except Exception as e:
            print(f"Fehler beim Laden: {e}")

    def get_raw_tables(self):
        """
        Gibt alle einzelnen Tabellen (DataFrames) in einem Dictionary zurück.
        Nützlich, um jede Tabelle einzeln zu inspizieren.
        """
        # Sicherstellen, dass Daten geladen sind
        if self.df_downloads is None:
            self.load_data()

        # Wir verpacken alle Tabellen in einen Container (Dictionary)
        raw_tables = {
            'Downloads': self.df_downloads,
            'Signups': self.df_signups,
            'Requests': self.df_requests,
            'Transactions': self.df_transactions,
            'Reviews': self.df_reviews
        }

        return raw_tables

    def merge_all_data(self):
        """
        Verbindet alle Tabellen mittels LEFT JOINS zu einem großen Funnel-DataFrame.
        """
        if self.df_downloads is None:
            self.load_data()

        print("Starte Merging der Tabellen...")

        # 1. Downloads mit Signups (Verbindung: app_download_key <-> session_id)
        self.df_funnel = pd.merge(
            self.df_downloads,
            self.df_signups,
            how='left',
            left_on='app_download_key',
            right_on='session_id'
        )

        # 2. Requests hinzufügen (Verbindung: user_id)
        # Hier bleibt user_id erhalten, weil wir DARÜBER mergen
        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_requests,
            how='left',
            on='user_id'
        )

        # 3. Transactions hinzufügen (Verbindung: ride_id)
        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_transactions,
            how='left',
            on='ride_id'
        )

        # 4. Reviews hinzufügen (Verbindung: ride_id)
        # ACHTUNG: Reviews hat auch eine 'user_id'. Da wir über 'ride_id' mergen,
        # entstehen hier 'user_id_x' und 'user_id_y'.
        self.df_funnel = pd.merge(
            self.df_funnel,
            self.df_reviews,
            how='left',
            on='ride_id'
        )

        # --- BEREINIGUNG DER NAMEN ---

        # Driver ID reparieren
        if 'driver_id_x' in self.df_funnel.columns:
            self.df_funnel.rename(columns={'driver_id_x': 'driver_id'}, inplace=True)

        # WICHTIG: User ID reparieren (das hat den Fehler verursacht)
        if 'user_id_x' in self.df_funnel.columns:
            self.df_funnel.rename(columns={'user_id_x': 'user_id'}, inplace=True)

        print(f"Merging abgeschlossen. Master-Table Größe: {self.df_funnel.shape}")
        return self.df_funnel

    def analyze_ride_duration_quality(self):
        """
        Analysiert die Fahrtdauer auf Ausreißer (Outliers),
        um zu prüfen, ob der Durchschnittswert plausibel ist.
        """
        if self.df_requests is None:
            self.load_data()

        # 1. Wir berechnen temporär die Dauer für alle Fahrten
        durations = (self.df_requests['dropoff_ts'] - self.df_requests['pickup_ts']).dt.total_seconds() / 60

        # 2. Wir erstellen einen statistischen Bericht
        stats_report = durations.describe()

        # 3. Wir suchen nach extremen Ausreißern (z.B. Fahrten über 5 Stunden)
        # Wir schauen uns die Rohdaten an, wo die Dauer > 300 Minuten ist
        long_rides = durations[durations > 300].count()
        negative_rides = durations[durations < 0].count()

        return stats_report, long_rides, negative_rides

    def get_warmup_stats(self):
        """
        Beantwortet die spezifischen Warm-up Fragen aus der Aufgabe.
        """
        if self.df_requests is None:
            self.load_data()

        stats = {}

        # 1. Downloads & 2. Signups
        stats['1_downloads'] = len(self.df_downloads)
        stats['2_signups'] = len(self.df_signups)

        # 3. Wie viele Fahrten wurden angefordert?
        # Einfach die Anzahl der Zeilen in ride_requests
        stats['3_rides_requested'] = len(self.df_requests)

        # 4. Wie viele Fahrten wurden abgeschloss
        # Abgeschlossen heißt: Es gibt einen Dropoff-Zeitstempel (nicht leer/NaN)
        stats['4_rides_completed'] = self.df_requests['dropoff_ts'].count()

        # 5. Wie viele eindeutige Benutzer haben eine Fahrt angefordert?
        # .nunique() zählt jeden User nur einmal, auch wenn er 10 Fahrten gemacht hat
        stats['5_unique_users_requesting'] = self.df_requests['user_id'].nunique()

        # 6. Durchschnittliche Dauer einer Fahrt (Abholung bis Absetzung)
        # Wir berechnen erst die Differenz für jede Fahrt und nehmen dann den Mittelwert (.mean)
        duration = (self.df_requests['dropoff_ts'] - self.df_requests['pickup_ts']).dt.total_seconds() / 60
        stats['6_avg_duration_minutes'] = round(duration.mean(), 2)

        # 7. Wie viele Fahrten angenommen? (Fahrerakzeptanz-Zeitstempel ist da)
        stats['7_rides_accepted'] = self.df_requests['accept_ts'].count()

        # 8. Umsatz insgesamt?
        stats['8_total_revenue'] = self.df_transactions['purchase_amount_usd'].sum()

        # 9. Anfragen pro Plattform
        # .value_counts() zählt, wie oft "ios", "android", "web" vorkommt
        stats['9_platform_counts'] = self.df_downloads['platform'].value_counts().to_dict()

        return stats

    def calculate_funnel_steps(self):
        """
        Berechnet die Anzahl der eindeutigen Nutzer (Unique Users) für jede Stufe des Funnels.
        Ziel: Herausfinden, wo wir Nutzer verlieren.
        """
        if self.df_funnel is None:
            self.merge_all_data()

        # Wir nutzen Sets oder nunique(), um sicherzustellen, dass wir jeden User nur 1x zählen.

        # 1. Download: Alle eindeutigen Download-Keys
        step_1_downloads = self.df_funnel['app_download_key'].nunique()

        # 2. Signup: Alle eindeutigen User-IDs (die nicht leer sind)
        step_2_signups = self.df_funnel['user_id'].nunique()

        # 3. Request: User, die mindestens einmal eine Zeit bei 'request_ts' haben
        # Wir filtern die Tabelle: Nur Zeilen, wo request_ts NICHT leer ist
        users_requested = self.df_funnel[self.df_funnel['request_ts'].notna()]['user_id'].nunique()

        # 4. Accepted: User, bei denen mindestens einmal ein Fahrer angenommen hat ('accept_ts')
        users_accepted = self.df_funnel[self.df_funnel['accept_ts'].notna()]['user_id'].nunique()

        # 5. Completed: User, die mindestens eine Fahrt beendet haben ('dropoff_ts')
        users_completed = self.df_funnel[self.df_funnel['dropoff_ts'].notna()]['user_id'].nunique()

        # 6. Payment: User, die bezahlt haben (Eintrag in der Transactions-Spalte 'charge_status' ist da)
        # Wir schauen, ob charge_status 'Approved' ist (oder generell existiert)
        users_paid = self.df_funnel[self.df_funnel['charge_status'] == 'Approved']['user_id'].nunique()

        # 7. Review: User, die eine Bewertung abgegeben haben ('review_id' ist nicht leer)
        users_reviewed = self.df_funnel[self.df_funnel['review_id'].notna()]['user_id'].nunique()

        # Wir packen das in ein schönes Dictionary für die Grafik
        funnel_data = {
            'steps': ['Downloads', 'Signups', 'Requests', 'Accepted', 'Completed', 'Payment', 'Reviews'],
            'counts': [step_1_downloads, step_2_signups, users_requested, users_accepted, users_completed, users_paid,
                       users_reviewed]
        }

        return funnel_data

    def analyze_dropoff_gap(self):
        """
        Untersucht, warum Fahrten akzeptiert, aber nicht abgeschlossen werden.
        Prüft auf Stornierungen (Cancellations).
        """
        # Sicherstellen, dass Daten da sind
        if self.df_requests is None:
            self.load_data()

        print("\n" + "-" * 40)
        print("ANALYSE: WARUM DER ABBRUCH NACH 'ACCEPTED'?")
        print("-" * 40)

        # 1. Wir suchen Fahrten: Fahrer hat JA gesagt (accept_ts da), aber Fahrt nicht beendet (dropoff_ts fehlt)
        # Das sind die "Geisterfahrten", die im Funnel fehlen
        problem_rides = self.df_requests[
            (self.df_requests['accept_ts'].notna()) &
            (self.df_requests['dropoff_ts'].isna())
            ]

        count_problems = len(problem_rides)
        print(f"1. Fahrten akzeptiert aber nicht beendet: {count_problems}")

        # 2. Wir prüfen: Haben diese Fahrten einen Stornierungs-Stempel (cancel_ts)?
        cancelled_count = problem_rides['cancel_ts'].notna().sum()
        print(f"2. Davon offiziell storniert (cancel_ts): {cancelled_count}")

        # 3. Prozentrechnung für den Report
        if count_problems > 0:
            quote = (cancelled_count / count_problems) * 100
            print(f"-> Das sind {quote:.1f}% der Fälle!")
        else:
            print("-> Keine Fälle gefunden.")

        print("-" * 40)

    def analyze_cancellation_reasons(self):
        """
        Analysiert Wartezeiten, um den Grund für Stornierungen zu finden.
        Vergleicht:
        A) Wie lange haben erfolgreiche Kunden auf den Fahrer gewartet? (Accept -> Pickup)
        B) Wie lange haben Abbrecher gewartet, bevor sie storniert haben? (Accept -> Cancel)
        """
        if self.df_requests is None:
            self.load_data()

        print("\n" + "-" * 50)
        print("      DEEP DIVE: WARTEZEITEN & STORNIERUNGEN      ")
        print("-" * 50)

        # 1. Erfolgreiche Fahrten: Zeit von 'Akzeptiert' bis 'Abholung'
        completed_rides = self.df_requests[self.df_requests['dropoff_ts'].notna()].copy()

        # Berechnung: Ankunftszeit des Fahrers in Minuten
        completed_rides['wait_time'] = (completed_rides['pickup_ts'] - completed_rides[
            'accept_ts']).dt.total_seconds() / 60
        avg_wait_completed = completed_rides['wait_time'].mean()

        print(f"Ø Wartezeit bei erfolgreichen Fahrten:  {avg_wait_completed:.2f} Minuten")

        # 2. Stornierte Fahrten: Zeit von 'Akzeptiert' bis 'Storniert'
        # Wir nehmen nur die Fahrten, die nach Accept storniert wurden (die "Problemfälle" von vorhin)
        cancelled_rides = self.df_requests[
            (self.df_requests['accept_ts'].notna()) &
            (self.df_requests['dropoff_ts'].isna()) &
            (self.df_requests['cancel_ts'].notna())
            ].copy()

        # Berechnung: Wie lange hat der Kunde gewartet, bevor er den Knopf gedrückt hat?
        cancelled_rides['patience_time'] = (cancelled_rides['cancel_ts'] - cancelled_rides[
            'accept_ts']).dt.total_seconds() / 60
        avg_wait_cancelled = cancelled_rides['patience_time'].mean()

        print(f"Ø Wartezeit vor Stornierung (Geduld): {avg_wait_cancelled:.2f} Minuten")

        # 3. Vergleich der Extreme (z.B. Wie viele haben länger als 10 Min gewartet?)
        long_waiters = cancelled_rides[cancelled_rides['patience_time'] > 10]
        print(
            f"Anzahl Stornierer, die länger als 10 Min gewartet haben: {len(long_waiters)} ({len(long_waiters) / len(cancelled_rides) * 100:.1f}%)")

        # 4. Check nach Tageszeit: Wann passieren Stornierungen? (Rush Hour?)
        # Wir extrahieren die Stunde aus dem cancel_ts
        cancelled_rides['hour'] = cancelled_rides['cancel_ts'].dt.hour
        busy_hours = cancelled_rides['hour'].value_counts().sort_index()

        # Wir geben die Top 3 Stunden mit den meisten Stornierungen aus
        top_hours = cancelled_rides['hour'].value_counts().head(3)
        print("\nTop 3 Stunden mit den meisten Stornierungen:")
        print(top_hours)

        return avg_wait_completed, avg_wait_cancelled

    def get_platform_metrics(self):
        """
        Analysiert den Funnel getrennt nach Plattform (ios, android, web).
        Ziel: Herausfinden, wo das Marketing-Budget hin soll.
        """
        if self.df_funnel is None:
            self.merge_all_data()

        # Wir gruppieren den Master-Table nach Plattform
        # Wir zählen Unique Users für Downloads und Completed Rides (Bezahlende Kunden)

        platform_stats = self.df_funnel.groupby('platform').agg({
            'app_download_key': 'nunique',  # Wie viele Downloads?
            'dropoff_ts': lambda x: x.notna().sum()
            # Wie viele abgeschlossene Fahrten? (Hier zählen wir Fahrten, nicht User, um Umsatzpotenzial zu sehen)
        }).reset_index()

        # Spalten umbenennen für schönere Ausgabe
        platform_stats.columns = ['Platform', 'Downloads', 'Completed_Rides']

        # Conversion Rate berechnen: (Fahrten / Downloads)
        # Achtung: Das ist eine vereinfachte Metrik ("Wie viele Fahrten entstehen pro Download")
        platform_stats['Conversion_Rate'] = (platform_stats['Completed_Rides'] / platform_stats['Downloads']) * 100

        return platform_stats

    def get_funnel_by_age(self):
        """
        Berechnet den Funnel getrennt nach Altersgruppen.
        Startet erst bei Signups, da wir bei Downloads das Alter noch nicht kennen.
        """
        if self.df_funnel is None:
            self.merge_all_data()

        # Wir filtern Daten ohne Altersangabe raus
        df_age = self.df_funnel[self.df_funnel['age_range'].notna()]

        # Wir bereiten eine Liste vor, um die Ergebnisse zu sammeln
        results = []

        # Wir gehen jede Altersgruppe durch (z.B. "18-24", "25-34"...)
        all_groups = df_age['age_range'].unique()

        for group in all_groups:
            # Nur Daten dieser Gruppe
            group_data = df_age[df_age['age_range'] == group]

            # Die Zahlen berechnen (Unique Users)
            signups = group_data['user_id'].nunique()
            requests = group_data[group_data['request_ts'].notna()]['user_id'].nunique()
            completed = group_data[group_data['dropoff_ts'].notna()]['user_id'].nunique()
            reviews = group_data[group_data['review_id'].notna()]['user_id'].nunique()

            # Speichern
            results.append({
                'Age_Group': group,
                '1_Signups': signups,
                '2_Requests': requests,
                '3_Completed': completed,
                '4_Reviews': reviews
            })

        # Daraus machen wir eine schöne Tabelle
        df_results = pd.DataFrame(results)

        # Sortieren nach Altersgruppe, damit es im Diagramm ordentlich aussieht
        df_results = df_results.sort_values('Age_Group')

        return df_results

    def analyze_surge_demand(self):
        """
        Analysiert die Nachfrage nach Tageszeit (Stunden),
        um Potenzial für Surge Pricing (Preiserhöhungen) zu finden.
        """
        if self.df_requests is None:
            self.load_data()

        # Wir nehmen den Zeitstempel der Anfrage
        # Sicherstellen, dass es datetime ist (falls noch nicht geschehen)
        self.df_requests['request_ts'] = pd.to_datetime(self.df_requests['request_ts'])

        # Wir extrahieren die Stunde (0-23)
        # Wir erstellen eine Kopie, um Warnungen zu vermeiden
        df_temp = self.df_requests[['request_ts']].copy()
        df_temp['hour'] = df_temp['request_ts'].dt.hour

        # Zählen: Wie viele Anfragen pro Stunde?
        hourly_demand = df_temp['hour'].value_counts().sort_index()

        return hourly_demand
