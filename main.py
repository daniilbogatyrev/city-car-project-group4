from funnel_utility import CityCarDataHandler
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

pd.set_option('display.float_format', lambda x: '%.2f' % x)


def main():
    data_handler = CityCarDataHandler()
    data_handler.load_data()

    # Übersicht der Tabellen
    print("\n" + "=" * 50)
    print("      ÜBERSICHT DER EINZELNEN TABELLEN      ")
    print("=" * 50)

    all_tables = data_handler.get_raw_tables()

    for name, table in all_tables.items():
        print(f"Tabelle: {name}")
        print(f" - Zeilen:  {len(table)}")
        print(f" - Spalten: {len(table.columns)}")
        print(f" - Spaltennamen: {table.columns.tolist()}")
        print("-" * 30)

    # Warm-Up Statistiken
    answers = data_handler.get_warmup_stats()

    print("\n" + "=" * 40)
    print("      WARM-UP FRAGEN & ANTWORTEN      ")
    print("=" * 40)
    print(f"1. Downloads gesamt:          {answers['1_downloads']}")
    print(f"2. Anmeldungen (Signups):     {answers['2_signups']}")
    print("-" * 40)
    print(f"3. Fahrten angefordert:       {answers['3_rides_requested']}")
    print(f"4. Fahrten abgeschlossen:     {answers['4_rides_completed']}")
    print(f"5. User mit Fahrtanfragen:    {answers['5_unique_users_requesting']}")
    print("-" * 40)
    print(f"6. Ø Fahrtdauer:              {answers['6_avg_duration_minutes']} Minuten")
    print(f"7. Fahrten akzeptiert:        {answers['7_rides_accepted']}")
    print(f"8. Gesamtumsatz:              ${answers['8_total_revenue']:,.2f}")
    print("-" * 40)
    print("9. Downloads pro Plattform:")
    for plattform, anzahl in answers['9_platform_counts'].items():
        print(f"   - {plattform}: {anzahl}")
    print("=" * 40)

    # Data Quality Check
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

    # Funnel-Analyse
    print("Berechne Funnel-Daten...")
    funnel_data = data_handler.calculate_funnel_steps()

    colors = [
       "#08306b", 
        "#08519c", 
        "#2171b5",
        "#4292c6",  
        "#d62728", 
        "#6baed6", 
        "#9ecae1"   
    ]

    connector_style = {"line": {"color": "#bdc3c7", "dash": "dot", "width": 2}}
    marker_line_style = {"width": 1, "color": "#ecf0f1"}

    # Konversionsrate Accepted -> Completed
    acc_val = funnel_data['counts'][3]
    comp_val = funnel_data['counts'][4]
    conv_rate = (comp_val / acc_val) * 100 if acc_val > 0 else 0

    # Chart 1: Detaillierter Funnel
    print("Erstelle detaillierten Funnel...")

    fig1 = go.Figure(go.Funnel(
        y=funnel_data['steps'],
        x=funnel_data['counts'],
        textposition="inside",
        textinfo="value+percent initial+percent previous",
        opacity=0.85,
        marker={"color": colors, "line": marker_line_style},
        connector=connector_style,
        textfont=dict(family="Arial", size=16, color="white")
    ))

    fig1.update_layout(
        title="1. Funnel Analyse: Hoher Verlust nach Annahme der Fahrt",
        template="plotly_white",
        margin=dict(r=160),
        font=dict(family="Arial", size=14, color="#2c3e50"),

    )

    fig1.add_annotation(
        x=comp_val,
        y="Completed",
        text=f"<b>ACHTUNG!</b><br>Nur {conv_rate:.1f}% der akzeptierten<br>Fahrten werden beendet.",
        showarrow=True,
        arrowhead=2,
        arrowcolor="#d62728",
        arrowsize=1.5,
        ax=120,
        ay=0,
        font=dict(color="#d62728", size=16,
        )
    )

    config = {

            'toImageButtonOptions': {
            'format': 'png',
           'filename': 'funnelanalysis',
            'height': 800,
            'width': 1200,
            'scale': 3

}

}


    fig1.show(config=config) 

# ---------------------------------------------------------
    # NEU: Analyse der Schmerzpunkte (Bar Chart mit Timestamps)
    # ---------------------------------------------------------
    print("Berechne Schmerzpunkte (Realität vs. Geduld)...")
    metrics = data_handler.get_patience_metrics()
    
    # Werte extrahieren
    val_search_real = metrics['Minuten'][0]
    val_search_pat  = metrics['Minuten'][1]
    val_pickup_real = metrics['Minuten'][2]
    val_pickup_pat  = metrics['Minuten'][3]
    
    fig_pain = go.Figure()

    # --- GRUPPE 1: FAHRERSUCHE ---
    # Realität: Wie lange dauert es bis zum Accept?
    fig_pain.add_trace(go.Bar(
        name='Realität (Median)',
        x=['<b>1. Fahrersuche</b><br><i>(Accept - Request)</i>'], # Timestamp Erklärung direkt im Label
        y=[val_search_real],
        marker_color='#3498db', # Blau
        text=[f"{val_search_real:.1f} min"],
        textposition='auto',
        offsetgroup=0
    ))
    
    # Geduld: Wie lange warten sie bis Cancel?
    fig_pain.add_trace(go.Bar(
        name='Geduld (Median Limit)',
        x=['<b>1. Fahrersuche</b><br><i>(Accept - Request)</i>'],
        y=[val_search_pat],
        marker_color='#95a5a6', # Grau
        text=[f"{val_search_pat:.1f} min"],
        textposition='auto',
        offsetgroup=1
    ))

    # --- GRUPPE 2: ABHOLUNG ---
    # Realität: Wie lange braucht der Fahrer?
    fig_pain.add_trace(go.Bar(
        name='Realität (Median)', # Gleicher Legenden-Name gruppiert automatisch
        x=['<b>2. Abholung</b><br><i>(Pickup - Accept)</i>'], # Timestamp Erklärung
        y=[val_pickup_real],
        marker_color='#e74c3c', # Rot (Signal für Problem)
        text=[f"{val_pickup_real:.1f} min"],
        textposition='auto',
        offsetgroup=0,
        showlegend=False
    ))
    
    # Geduld: Wie lange warten sie nach Accept?
    fig_pain.add_trace(go.Bar(
        name='Geduld (Median Limit)',
        x=['<b>2. Abholung</b><br><i>(Pickup - Accept)</i>'],
        y=[val_pickup_pat],
        marker_color='#95a5a6',
        text=[f"{val_pickup_pat:.1f} min"],
        textposition='auto',
        offsetgroup=1,
        showlegend=False
    ))

    # Annotation für den GAP (Lücke)
    gap = val_pickup_real - val_pickup_pat
    fig_pain.add_annotation(
        x='<b>2. Abholung</b><br><i>(Pickup - Accept)</i>',
        y=max(val_pickup_real, val_pickup_pat) + 1.5,
        text=f"<b>GAP: +{gap:.1f} min</b><br>(Fahrer kommt zu spät!)",
        showarrow=False,
        font=dict(color="#c0392b", size=14)
    )

    fig_pain.update_layout(
        title='<b>Schmerzpunkt-Analyse:</b> Wo verlieren wir Kunden?',
        yaxis_title='Minuten (Median)',
        barmode='group',
        template='plotly_white',
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)'),
        margin=dict(b=100) # Platz unten für Labels lassen
    )

    # Optional: Ein technischer Hinweis unter dem Graphen
    fig_pain.add_annotation(
        text="Basis: <i>Accept - Request</i> (Suche) vs. <i>Pickup - Accept</i> (Abholung). Geduld gemessen an <i>Cancel TS</i>.",
        xref="paper", yref="paper",
        x=0.5, y=-0.25,
        showarrow=False,
        font=dict(size=10, color="gray")
    )

    fig_pain.show()










    # Plattform-Analyse
    print("Berechne Plattform-Daten...")
    df_platform = data_handler.get_platform_metrics()

    print("\n" + "=" * 40)
    print("      PLATTFORM VERGLEICH      ")
    print("=" * 40)
    print(df_platform)

    fig_platform = px.bar(
        df_platform,
        x='Platform',
        y=['Downloads', 'Completed_Rides'],
        barmode='group',
        title='Vergleich: Downloads vs. Abgeschlossene Fahrten nach Plattform',
        labels={'value': 'Anzahl', 'variable': 'Metrik'},
        color_discrete_sequence=['#34495e', '#2ecc71']
    )

    fig_platform.show()

    # Alters-Analyse
    print("Berechne Alters-Strukturen...")
    df_age = data_handler.get_funnel_by_age()

    print("\n" + "=" * 40)
    print("      ZIELGRUPPEN ANALYSE      ")
    print("=" * 40)
    print(df_age)

    df_plot = df_age.melt(id_vars='Age_Group', var_name='Stage', value_name='Users')

    fig_age = px.bar(
        df_plot,
        x='Age_Group',
        y='Users',
        color='Stage',
        barmode='group',
        title='Performance nach Altersgruppen (Wer sind unsere Top-Kunden?)',
        color_discrete_sequence=px.colors.sequential.Viridis
    )

    fig_age.show()

    # Surge Pricing Analyse
    print("Analysiere Nachfrage-Verteilung für Surge Pricing...")
    hourly_data = data_handler.analyze_surge_demand()

    df_plot = pd.DataFrame({
        'Stunde': hourly_data.index,
        'Anfragen': hourly_data.values
    })

    print("\n" + "=" * 40)
    print("      NACHFRAGE PRO STUNDE      ")
    print("=" * 40)
    print(df_plot)

    fig_surge = px.line(
        df_plot,
        x='Stunde',
        y='Anfragen',
        title='Verteilung der Fahrtanfragen über den Tag (Surge Pricing Analyse)',
        markers=True,
        labels={'Stunde': 'Uhrzeit (0-23 Uhr)', 'Anfragen': 'Anzahl Requests'}
    )

    fig_surge.update_xaxes(tickmode='linear', dtick=1)
    fig_surge.update_traces(fill='tozeroy', line_color='#e74c3c')

    fig_surge.show()

if __name__ == "__main__":
    main()