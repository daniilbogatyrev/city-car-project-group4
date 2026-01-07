from funnel_utility import CityCarDataHandler
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

pd.set_option('display.float_format', lambda x: '%.2f' % x)

def main():
    data_handler = CityCarDataHandler()
    data_handler.load_data()

    # ---------------------------------------------------------
    # Zugriff auf die einzelnen Tabellen
    # ---------------------------------------------------------
    print("\n" + "=" * 50)
    print("      ÜBERSICHT DER EINZELNEN TABELLEN      ")
    print("=" * 50)

    # Wir rufen unsere neue Funktion auf
    all_tables = data_handler.get_raw_tables()

    # Jetzt gehen wir durch das Dictionary durch (Schleife)
    # name = z.B. 'Downloads', table = der echte DataFrame
    for name, table in all_tables.items():
        print(f"Tabelle: {name}")
        print(f" - Zeilen:  {len(table)}")
        print(f" - Spalten: {len(table.columns)}")
        print(f" - Spaltennamen: {table.columns.tolist()}")
        print("-" * 30)

    # Wir rufen die Antworten ab
    answers = data_handler.get_warmup_stats()

    print("\n" + "=" * 40)
    print("      WARM-UP FRAGEN & ANTWORTEN      ")
    print("=" * 40)

    print(f"1. Downloads gesamt:        {answers['1_downloads']}")
    print(f"2. Anmeldungen (Signups):   {answers['2_signups']}")
    print("-" * 40)
    print(f"3. Fahrten angefordert:     {answers['3_rides_requested']}")
    print(f"4. Fahrten abgeschlossen:   {answers['4_rides_completed']}")
    print(f"5. User, die Fahrten wollten: {answers['5_unique_users_requesting']}")
    print("-" * 40)
    print(f"6. Ø Dauer einer Fahrt:     {answers['6_avg_duration_minutes']} Minuten")
    print(f"7. Fahrten von Fahrern akzeptiert: {answers['7_rides_accepted']}")
    print(f"8. Gesamtumsatz:            ${answers['8_total_revenue']:,.2f}")
    print("-" * 40)
    print("9. Downloads pro Plattform:")
    for plattform, anzahl in answers['9_platform_counts'].items():
        print(f"   - {plattform}: {anzahl}")
    print("=" * 40)

    print("\n" + "=" * 50)
    print("      DATA QUALITY CHECK: FAHRTDAUER      ")
    print("=" * 50)

    report, long_rides_count, negative_rides_count = data_handler.analyze_ride_duration_quality()

    print("Statistischer Bericht (in Minuten):")
    print(report)
    print("-" * 30)
    print(f"Anzahl Fahrten über 5 Stunden (300 Min): {long_rides_count}")
    print(f"Anzahl Fahrten mit negativer Zeit:       {negative_rides_count}")
    print("=" * 50)

    #------------------------------------------------------------------#
    print("Berechne Funnel-Daten...")
    # Die Daten holen
    funnel_data = data_handler.calculate_funnel_steps()

    # --- DESIGN DEFINITIONEN ---
    # Von kühlem Blau (Start) über Grün/Gelb zu warmem Rot (Ende)
    cool_to_warm_colors = [
        "#34495e",  # Cool Dark Blue (Downloads)
        "#3498db",  # Blue (Signups)
        "#1abc9c",  # Teal (Requests)
        "#2ecc71",  # Green (Accepted)
        "#f1c40f",  # Yellow (Completed)
        "#e67e22",  # Orange (Payment)
        "#e74c3c"  # Warm Red (Reviews)
    ]

    # Wir verwenden diese Palette
    my_colors = cool_to_warm_colors

    # Styling für die Verbindungslinien
    connector_style = {"line": {"color": "#bdc3c7", "dash": "dot", "width": 2}}
    # Styling für den Rahmen der Balken
    marker_line_style = {"width": 1, "color": "#ecf0f1"}

    # ---------------------------------------------------------
    # GRAFIK 1: Prozent des Vorherigen (Wo ist der Drop-Off?)
    # ---------------------------------------------------------
    fig_prev = go.Figure(go.Funnel(
        name='Step-by-Step',
        y=funnel_data['steps'],
        x=funnel_data['counts'],
        textposition="inside",
        textinfo="value+percent previous",
        opacity=0.85,  # Etwas weniger transparent, damit die Farben leuchten

        marker={
            "color": my_colors,
            "line": marker_line_style
        },
        connector=connector_style
    ))

    fig_prev.update_layout(
        title_text="Funnel Analyse: Drop-off pro Schritt (Cool-to-Warm)",
        title_x=0.5,
        yaxis_title="Funnel Stufen",
        # Ein sauberer, weißer Hintergrund
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    fig_prev.show()

    # ---------------------------------------------------------
    # GRAFIK 2: Prozent der Gesamtheit (Wie viele schaffen es bis zum Ende?)
    # ---------------------------------------------------------
    print("Erstelle Grafik für 'Prozent der Gesamtheit'...")

    # Für die zweite Grafik nutzen wir dieselben Farben für Konsistenz
    fig_total = go.Figure(go.Funnel(
        name='Overall Conversion',
        y=funnel_data['steps'],
        x=funnel_data['counts'],
        textposition="inside",
        textinfo="value+percent initial",
        opacity=0.85,

        marker={
            "color": my_colors,
            "line": marker_line_style
        },
        connector=connector_style
    ))

    fig_total.update_layout(
        title_text="Funnel Analyse: Gesamt-Konversion (Percent of Total)",
        title_x=0.5,
        yaxis_title="Funnel Stufen",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )

    fig_total.show()
    # Dropoff check
    """
    Untersucht, warum Fahrten akzeptiert, aber nicht abgeschlossen werden.
    Prüft auf Stornierungen (Cancellations).
    """
    data_handler.analyze_dropoff_gap()
    """
    Analysiert Wartezeiten, um den Grund für Stornierungen zu finden.
    Vergleicht:
    A) Wie lange haben erfolgreiche Kunden auf den Fahrer gewartet? (Accept -> Pickup)
    B) Wie lange haben Abbrecher gewartet, bevor sie storniert haben? (Accept -> Cancel)
    """
    data_handler.analyze_cancellation_reasons()

    # --- PLATTFORM ANALYSE ---
    print("Berechne Plattform-Daten...")
    df_platform = data_handler.get_platform_metrics()

    print("\n" + "=" * 40)
    print("      PLATTFORM VERGLEICH      ")
    print("=" * 40)
    print(df_platform)

    # Grafik erstellen: Vergleich der Downloads pro Plattform
    # Wir nutzen ein einfaches Balkendiagramm
    fig_platform = px.bar(
        df_platform,
        x='Platform',
        y=['Downloads', 'Completed_Rides'],
        barmode='group',  # Balken nebeneinander
        title='Vergleich: Downloads vs. Abgeschlossene Fahrten nach Plattform',
        labels={'value': 'Anzahl', 'variable': 'Metrik'},
        color_discrete_sequence=['#34495e', '#2ecc71']  # Dunkelblau für DL, Grün für Fahrten
    )

    fig_platform.show()

    # --- ALTERS ANALYSE ---
    print("Berechne Alters-Strukturen...")
    df_age = data_handler.get_funnel_by_age()

    print("\n" + "=" * 40)
    print("      ZIELGRUPPEN ANALYSE      ")
    print("=" * 40)
    print(df_age)

    # Um das Diagramm zu zeichnen, müssen wir die Tabelle etwas umformen ("Melten")
    # Das macht es für Plotly einfacher, die bunten Balken zu malen
    df_plot = df_age.melt(id_vars='Age_Group', var_name='Stage', value_name='Users')

    # Das Diagramm
    fig_age = px.bar(
        df_plot,
        x='Age_Group',
        y='Users',
        color='Stage',  # Jede Stufe bekommt eine Farbe
        barmode='group',  # Balken stehen nebeneinander
        title='Performance nach Altersgruppen (Wer sind unsere Top-Kunden?)',
        color_discrete_sequence=px.colors.sequential.Viridis  # Schöne Farbpalette
    )

    fig_age.show()

    # --- ALTERS ANALYSE ---
    print("Berechne Alters-Strukturen...")
    df_age = data_handler.get_funnel_by_age()

    print("\n" + "="*40)
    print("      ZIELGRUPPEN ANALYSE      ")
    print("="*40)
    print(df_age)

    # Um das Diagramm zu zeichnen, müssen wir die Tabelle etwas umformen ("Melten")
    # Das macht es für Plotly einfacher, die bunten Balken zu malen
    df_plot = df_age.melt(id_vars='Age_Group', var_name='Stage', value_name='Users')

    # Das Diagramm
    fig_age = px.bar(
        df_plot,
        x='Age_Group',
        y='Users',
        color='Stage', # Jede Stufe bekommt eine Farbe
        barmode='group', # Balken stehen nebeneinander
        title='Performance nach Altersgruppen (Wer sind unsere Top-Kunden?)',
        color_discrete_sequence=px.colors.sequential.Viridis # Schöne Farbpalette
    )

    fig_age.show()

    # --- SURGE PRICING ANALYSE ---
    print("Analysiere Nachfrage-Verteilung für Surge Pricing...")
    hourly_data = data_handler.analyze_surge_demand()

    # Daten für Plotly vorbereiten
    df_plot = pd.DataFrame({
        'Stunde': hourly_data.index,  # 0, 1, 2 ... 23
        'Anfragen': hourly_data.values
    })

    print("\n" + "=" * 40)
    print("      NACHFRAGE PRO STUNDE      ")
    print("=" * 40)
    print(df_plot)

    # Diagramm erstellen
    fig_surge = px.line(
        df_plot,
        x='Stunde',
        y='Anfragen',
        title='Verteilung der Fahrtanfragen über den Tag (Surge Pricing Analyse)',
        markers=True,  # Punkte auf der Linie anzeigen
        labels={'Stunde': 'Uhrzeit (0-23 Uhr)', 'Anfragen': 'Anzahl Requests'}
    )

    # Wir markieren die X-Achse schön mit allen Stunden
    fig_surge.update_xaxes(tickmode='linear', dtick=1)

    # Optional: Flächen unter der Kurve füllen für besseren Look
    fig_surge.update_traces(fill='tozeroy', line_color='#e74c3c')  # Rot wie

    fig_surge.show()


if __name__ == "__main__":
    main()

