#!/usr/bin/env python3
"""
DWD-Warnarchiv – sammelt amtliche Wetterwarnungen des Deutschen Wetterdienstes
(CAP v1.2, Open-Data-Server) und speichert sie dauerhaft in einer SQLite-Datenbank.

Hintergrund: Der DWD hält auf opendata.dwd.de nur ein rollierendes Fenster von
ca. 48 Stunden vor, ein Langzeitarchiv der CAP-Warnungen gibt es dort nicht.
Für spätere Auswertungen muss man also selbst archivieren.

Quellen (Landkreisebene, deutsch):
  STAT  https://opendata.dwd.de/weather/alerts/cap/DISTRICT_DWD_STAT/
        Momentaufnahme aller gerade gültigen Warnungen.
  DIFF  https://opendata.dwd.de/weather/alerts/cap/DISTRICT_DWD_DIFF/
        Änderungen seit der vorigen Datei (neu / Update / Cancel), etwa alle
        10 Minuten. Erfasst auch kurzlebige Warnungen, die zwischen zwei
        STAT-Momentaufnahmen ausgegeben und wieder aufgehoben wurden.
Beide Produkte werden ausgewertet; jede Warnung (CAP-identifier) wird nur
einmal gespeichert.

Nur Python-Standardbibliothek, keine Zusatzpakete nötig (Python >= 3.8).

Aufruf:
  python dwd_warnarchiv.py                 # neue Dateien (STAT + DIFF) holen und speichern
  python dwd_warnarchiv.py --export        # zusätzlich CSV-Dateien für Excel schreiben
  python dwd_warnarchiv.py --file X.zip    # lokale ZIP-Datei einlesen (Test)
  python dwd_warnarchiv.py --alle-regionen # ganz Deutschland statt nur Filtergebiet
"""

import argparse
import csv
import io
import re
import sqlite3
import sys
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------
# Konfiguration
# --------------------------------------------------------------------------
BASIS = "https://opendata.dwd.de/weather/alerts/cap/"
PRODUKTE = {
    # Kürzel: (Verzeichnis, Muster der deutschen Dateien)
    "STAT": ("DISTRICT_DWD_STAT/", re.compile(
        r'href="(Z_CAP_C_EDZW_(\d{14})_PVW_STATUS_PREMIUMDWD_DISTRICT_DE\.zip)"')),
    "DIFF": ("DISTRICT_DWD_DIFF/", re.compile(
        r'href="(Z_CAP_C_EDZW_(\d{14})_PVW_DIFFERENCE_PREMIUMDWD_DISTRICT_DE\.zip)"')),
}

ORDNER = Path(__file__).resolve().parent
DB_PFAD = ORDNER / "dwd_warnungen.sqlite"
LOG_PFAD = ORDNER / "dwd_warnarchiv.log"

# Regionsfilter. Eine Warnung wird gespeichert, wenn EINE der
# beiden Bedingungen zutrifft (doppelt abgesichert):
#  a) WARNCELLID steht in REGION_WARNCELLS
#  b) areaDesc enthält einen der Namen aus REGION_NAMEN
# Bitte die IDs einmalig gegen die offizielle Liste prüfen:
# https://www.dwd.de/DE/leistungen/opendata/help/warnungen/cap_warncellids_csv.csv
REGION_WARNCELLS = {
    "109663000",
    "809663000",
    "109679000",
    "109675000",
    "109677000",
}
REGION_NAMEN = ("Würzburg", "Kitzingen", "Main-Spessart")

# Nach längerem Ausfall (PC aus) werden verpasste Snapshots nachgeholt,
# solange sie noch auf dem Server liegen (~48 h). Obergrenze pro Lauf:
MAX_NACHHOLEN = 300

HTTP_TIMEOUT = 60
USER_AGENT = "DWD-Warnarchiv/1.0 (Python urllib)"


# --------------------------------------------------------------------------
# Hilfsfunktionen
# --------------------------------------------------------------------------
def log(text):
    zeile = f"{datetime.now().isoformat(timespec='seconds')}  {text}"
    print(zeile)
    try:
        with open(LOG_PFAD, "a", encoding="utf-8") as f:
            f.write(zeile + "\n")
    except OSError:
        pass


def hole(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        return r.read()


def lokal(tag):
    """Tag-Name ohne XML-Namespace."""
    return tag.rsplit("}", 1)[-1]


def kind(el, name):
    for c in el:
        if lokal(c.tag) == name:
            return c
    return None


def kinder(el, name):
    return [c for c in el if lokal(c.tag) == name]


def text(el, name):
    c = kind(el, name)
    return c.text.strip() if c is not None and c.text else None


def name_wert_paare(el, name):
    """<eventCode>/<parameter>/<geocode> -> Liste (valueName, value)."""
    paare = []
    for c in kinder(el, name):
        paare.append((text(c, "valueName"), text(c, "value")))
    return paare


# --------------------------------------------------------------------------
# Datenbank
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    dateiname        TEXT PRIMARY KEY,
    zeitstempel_utc  TEXT NOT NULL,      -- aus dem Dateinamen
    verarbeitet_utc  TEXT NOT NULL,
    anzahl_gesamt    INTEGER,            -- CAP-Meldungen bundesweit in der Datei
    anzahl_region    INTEGER,            -- davon im Filtergebiet
    produkt          TEXT                -- STAT oder DIFF
);

CREATE TABLE IF NOT EXISTS warnungen (
    identifier       TEXT PRIMARY KEY,   -- eindeutige CAP-ID
    sent             TEXT,
    msg_type         TEXT,               -- Alert / Update / Cancel
    status           TEXT,
    references_      TEXT,               -- Vorgänger-Meldungen (bei Update/Cancel)
    event            TEXT,               -- z. B. STURMBÖEN
    ii_code          TEXT,               -- DWD-Ereigniscode (eventCode II)
    gruppe           TEXT,               -- eventCode GROUP, z. B. WIND
    severity         TEXT,               -- Minor / Moderate / Severe / Extreme
    urgency          TEXT,
    certainty        TEXT,
    response_type    TEXT,
    effective        TEXT,
    onset            TEXT,
    expires          TEXT,
    headline         TEXT,
    description      TEXT,
    instruction      TEXT,
    parameter        TEXT,               -- z. B. Böen-/Niederschlagsangaben
    erstmals_gesehen TEXT,               -- UTC, erste Datei mit dieser Meldung
    zuletzt_gesehen  TEXT                -- UTC, letzte Datei mit dieser Meldung
);

CREATE TABLE IF NOT EXISTS warnung_gebiete (
    identifier       TEXT NOT NULL,
    warncellid       TEXT NOT NULL,
    area_desc        TEXT,
    PRIMARY KEY (identifier, warncellid)
);

CREATE INDEX IF NOT EXISTS idx_w_onset ON warnungen(onset);
CREATE INDEX IF NOT EXISTS idx_w_event ON warnungen(event);
CREATE INDEX IF NOT EXISTS idx_g_cell  ON warnung_gebiete(warncellid);

-- Bequeme Auswertungssicht: eine Zeile pro Warnung und Gebiet
CREATE VIEW IF NOT EXISTS v_warnungen_gebiete AS
SELECT g.area_desc, g.warncellid, w.*
FROM warnungen w JOIN warnung_gebiete g USING (identifier);
"""


def db_oeffnen():
    con = sqlite3.connect(DB_PFAD)
    con.executescript(SCHEMA)
    # ältere Datenbanken (vor Einführung von DIFF) um Spalte "produkt" ergänzen
    spalten = [r[1] for r in con.execute("PRAGMA table_info(snapshots)")]
    if "produkt" not in spalten:
        con.execute("ALTER TABLE snapshots ADD COLUMN produkt TEXT")
        con.execute("UPDATE snapshots SET produkt = 'STAT' WHERE produkt IS NULL")
        con.commit()
    return con


# --------------------------------------------------------------------------
# CAP parsen
# --------------------------------------------------------------------------
def parse_cap(xml_bytes):
    root = ET.fromstring(xml_bytes)
    if lokal(root.tag) != "alert":
        return None

    infos = kinder(root, "info")
    # deutsche info bevorzugen
    info = next((i for i in infos if (text(i, "language") or "").startswith("de")),
                infos[0] if infos else None)
    if info is None:
        return None

    eventcodes = dict(name_wert_paare(info, "eventCode"))
    parameter = name_wert_paare(info, "parameter")

    gebiete = []
    for area in kinder(info, "area"):
        desc = text(area, "areaDesc")
        cells = [v for n, v in name_wert_paare(area, "geocode")
                 if n and n.upper() == "WARNCELLID" and v]
        if not cells:
            cells = [""]
        for c in cells:
            gebiete.append((c, desc))

    return {
        "identifier": text(root, "identifier"),
        "sent": text(root, "sent"),
        "msg_type": text(root, "msgType"),
        "status": text(root, "status"),
        "references_": text(root, "references"),
        "event": text(info, "event"),
        "ii_code": eventcodes.get("II"),
        "gruppe": eventcodes.get("GROUP"),
        "severity": text(info, "severity"),
        "urgency": text(info, "urgency"),
        "certainty": text(info, "certainty"),
        "response_type": text(info, "responseType"),
        "effective": text(info, "effective"),
        "onset": text(info, "onset"),
        "expires": text(info, "expires"),
        "headline": text(info, "headline"),
        "description": text(info, "description"),
        "instruction": text(info, "instruction"),
        "parameter": "; ".join(f"{n}={v}" for n, v in parameter if n) or None,
        "gebiete": gebiete,
    }


def im_filtergebiet(warnung, alle_regionen):
    if alle_regionen:
        return warnung["gebiete"]
    treffer = []
    for cell, desc in warnung["gebiete"]:
        if cell in REGION_WARNCELLS or (desc and any(n in desc for n in REGION_NAMEN)):
            treffer.append((cell, desc))
    return treffer


# --------------------------------------------------------------------------
# Snapshot verarbeiten
# --------------------------------------------------------------------------
def verarbeite_zip(con, produkt, dateiname, zeitstempel, zip_bytes, alle_regionen):
    jetzt = datetime.now(timezone.utc).isoformat(timespec="seconds")
    snap_zeit = datetime.strptime(zeitstempel, "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc).isoformat()

    gesamt = 0
    region = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            try:
                w = parse_cap(zf.read(name))
            except ET.ParseError as e:
                log(f"  XML-Fehler in {name}: {e}")
                continue
            if not w or not w["identifier"]:
                continue
            gesamt += 1
            gebiete = im_filtergebiet(w, alle_regionen)
            if not gebiete:
                continue
            region += 1

            felder = {k: v for k, v in w.items() if k != "gebiete"}
            felder["erstmals_gesehen"] = snap_zeit
            felder["zuletzt_gesehen"] = snap_zeit
            spalten = ", ".join(felder)
            platzh = ", ".join("?" * len(felder))
            con.execute(
                f"INSERT INTO warnungen ({spalten}) VALUES ({platzh}) "
                "ON CONFLICT(identifier) DO UPDATE SET "
                "zuletzt_gesehen = MAX(zuletzt_gesehen, excluded.zuletzt_gesehen), "
                "erstmals_gesehen = MIN(erstmals_gesehen, excluded.erstmals_gesehen)",
                list(felder.values()),
            )
            con.executemany(
                "INSERT OR IGNORE INTO warnung_gebiete VALUES (?, ?, ?)",
                [(w["identifier"], c, d) for c, d in gebiete],
            )

    con.execute(
        "INSERT OR REPLACE INTO snapshots "
        "(dateiname, zeitstempel_utc, verarbeitet_utc, anzahl_gesamt, anzahl_region, produkt) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (dateiname, snap_zeit, jetzt, gesamt, region, produkt),
    )
    con.commit()
    if gesamt or produkt == "STAT":
        log(f"  [{produkt}] {dateiname}: {gesamt} Meldungen bundesweit, "
            f"{region} im Filtergebiet")


def lauf_produkt(con, produkt, alle_regionen):
    verzeichnis, muster = PRODUKTE[produkt]
    url = BASIS + verzeichnis
    try:
        html = hole(url).decode("utf-8", "replace")
    except Exception as e:
        log(f"[{produkt}] FEHLER beim Abruf der Verzeichnisliste: {e}")
        return 1

    dateien = sorted(set(muster.findall(html)), key=lambda x: x[1])
    if not dateien:
        log(f"[{produkt}] WARNUNG: keine passenden Dateien – hat der DWD das Format geändert?")
        return 1

    bekannt = {r[0] for r in con.execute(
        "SELECT dateiname FROM snapshots WHERE produkt = ?", (produkt,))}
    letzter = con.execute(
        "SELECT MAX(zeitstempel_utc) FROM snapshots WHERE produkt = ?",
        (produkt,)).fetchone()[0]

    if letzter is None:
        # Erstlauf: STAT nur neueste Momentaufnahme,
        # DIFF alles, was noch auf dem Server liegt (~48 h)
        offen = [dateien[-1]] if produkt == "STAT" else dateien
    else:
        letzter_ts = datetime.fromisoformat(letzter).strftime("%Y%m%d%H%M%S")
        offen = [d for d in dateien if d[0] not in bekannt and d[1] > letzter_ts]
    offen = offen[-MAX_NACHHOLEN:]

    if not offen:
        log(f"[{produkt}] Keine neue Datei.")
        return 0

    fehler = 0
    for dateiname, ts in offen:
        try:
            verarbeite_zip(con, produkt, dateiname, ts, hole(url + dateiname),
                           alle_regionen)
        except Exception as e:
            fehler += 1
            log(f"  [{produkt}] FEHLER bei {dateiname}: {e}")
    log(f"[{produkt}] {len(offen)} Datei(en) verarbeitet, {fehler} Fehler.")
    return 1 if fehler else 0


def lauf(alle_regionen):
    con = db_oeffnen()
    rc = 0
    for produkt in PRODUKTE:
        rc |= lauf_produkt(con, produkt, alle_regionen)
    con.close()
    return rc


def exportiere_csv():
    con = db_oeffnen()
    for tabelle, datei in (("v_warnungen_gebiete", "export_warnungen_gebiete.csv"),
                           ("snapshots", "export_snapshots.csv")):
        cur = con.execute(f"SELECT * FROM {tabelle} ORDER BY 1")
        pfad = ORDNER / datei
        # utf-8-sig + Semikolon: öffnet in deutschem Excel direkt korrekt
        with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([d[0] for d in cur.description])
            w.writerows(cur)
        log(f"Export: {pfad}")
    con.close()


def main():
    ap = argparse.ArgumentParser(description="DWD-Warnungen archivieren")
    ap.add_argument("--export", action="store_true", help="CSV-Export für Excel")
    ap.add_argument("--file", help="lokale CAP-ZIP-Datei einlesen (Test)")
    ap.add_argument("--alle-regionen", action="store_true",
                    help="ganz Deutschland speichern statt nur Filtergebiet")
    args = ap.parse_args()

    rc = 0
    if args.file:
        p = Path(args.file)
        m = re.search(r"_(\d{14})_", p.name)
        ts = m.group(1) if m else datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        produkt = "DIFF" if "_DIFFERENCE_" in p.name else "STAT"
        verarbeite_zip(db_oeffnen(), produkt, p.name, ts, p.read_bytes(),
                       args.alle_regionen)
    else:
        rc = lauf(args.alle_regionen)

    if args.export:
        exportiere_csv()
    return rc


if __name__ == "__main__":
    sys.exit(main())
