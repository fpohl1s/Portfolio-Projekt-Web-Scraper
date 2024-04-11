# Shopify-Scraper
## Architektur
![Bildschirmfoto 2024-04-11 um 12 34 18](https://github.com/fpohl1s/Portfolio-Projekt-Web-Scraper/assets/113839258/83c49bc3-2f00-48f6-bec3-d56dd12cfb33)

## Projektübersicht
Das Shopify-Scraper-Projekt ist ein umfassendes Tool, entwickelt, um Daten von verschiedenen Shopify-basierten Online-Shops effizient zu extrahieren. Es automatisiert den Prozess des Sammelns wichtiger Informationen wie Produktbeschreibungen, Preise, Varianten und Bilder, um Marktforschung, Preisvergleiche oder E-Commerce-Analysen zu unterstützen.

## Hauptmerkmale
- **Automatisierte Extraktion**: Sammelt automatisch Daten von mehreren Webseiten und speichert sie strukturiert.
- **Transformationslogik**: Bereinigt und transformiert die extrahierten Daten für eine einfachere Analyse und Verarbeitung.
- **Datenspeicherung**: Unterstützt das Speichern der Daten in verschiedenen Formaten und Datenbanken, einschließlich Pandas DataFrames, Parquet-Dateien und MySQL.
- **Skalierbarkeit**: Entworfen, um mit mehreren Shopify-Shops und großen Datenmengen umzugehen.
- **Apache Airflow Integration**: Nutzt Apache Airflow, um den Scraping-Prozess zu orchestrieren und zu automatisieren.
- **Datenvisualisierung:** Verwendung von Power BI, um die Daten aggregiert oder auf Produktebene visualisieren & analyisieren zu können.

## Technologien
- **Python:** Für den gesamten Scraping- und Datenverarbeitungsprozess.
- **Pandas:** Zur Datenmanipulation und -transformation.
- **SQLAlchemy:** Für die Interaktion mit MySQL-Datenbanken.
- **Apache Airflow:** Für die Workflow-Orchestrierung und Automatisierung.
- **BeautifulSoup:** Zum Parsen von HTML und Extrahieren von Daten.
- **Power BI:** Zur Visualisierung und Datenanlyse der gesamten Datenbasis.

## Anwendungsfälle
- **Marktforschung:** Gewinnen von Einblicken in Produktangebote und Markttrends auf Shopify-basierten Plattformen.
- **Preisüberwachung:** Verfolgen von Preisänderungen und Angeboten, um wettbewerbsfähige Preise zu gewährleisten.
- **E-Commerce-Analyse:** Analysieren von Produktverfügbarkeiten, Varianten und Bildern, um daraus bessere Geschäftsentscheidungen abzuleiten.

## Schnellstart
1. Klone das Repository
2. Erstelle eine MySQL Datenbank (local/cloud)
3. Initialisiere die drei Tabellen der Datenbank mit der `initialize_db.ipynb`-Datei
4. Lege die Dateien/Ordner `scraper_etl_dag.py, scraper.py, Hilfstabellen, Marken` in den Ordnerpfad `airflow/dags`
5. Erstelle in dem Ordner `dags` eine .env-Datei und hinterlege dort die Log-In Daten für die MySQL Datenbank
6. Füge in der `Hilfstabellen/Brand_URL.csv`-Datei die Shops und deren entsprechende URL hinzu (WICHITG: Die Websiten müssen Shopify Shops sein!)
7. Öffne die Airflow-Browser-Übersicht und aktiviere das `scraper_etl_dag`. Unter dem Reiter 'Graph' sollte die Ansicht wie folgt aussehen: ![Bildschirmfoto 2024-04-10 um 17 37 20](https://github.com/fpohl1s/Portfolio-Projekt-Web-Scraper/assets/113839258/8c165ea6-376f-44ac-8088-51abe22da991)
8. Öffne Power BI Desktop und erstelle mit den Log-In Daten eine Verbindung zu der MySQL Datenbank. Die Oberfläche kann anschließend beispielsweise wie folgend gestaltet werden: ![image](https://github.com/fpohl1s/Portfolio-Projekt-Web-Scraper/assets/113839258/0cf64b92-f788-4465-840f-c10a911cb5ce)

## Feedback
Falls du zu meinem Projekt Rückfragen, Anpassungsvorschläge oder Feedback hast, dann schreibe mir gerne auf [LinkedIn](https://www.linkedin.com/in/ferdinand-pohl-7384261a6/)!



