# teste12.py
# Multi-liga via URL do Sofascore (API oficial) + fallback ScraperFC.
# Sem argumentos: modo interativo -> lista ligas, você escolhe, e ele consulta a temporada mais recente.
# Flags úteis:
#   --list-leagues  : lista ligas aceitas pelo ScraperFC e sai
#   --list-aliases  : mostra mapeamentos (apelidos -> nome oficial) e sai
#   --url           : URL do Sofascore (tournament/season) p/ tentar API oficial
#   --league        : nome da liga p/ fallback (opcional com --url)
#   --year          : ano p/ fallback (opcional com --url)

import re
import time
import argparse
import requests
import pandas as pd
from typing import Optional, Tuple, List
from requests.adapters import HTTPAdapter, Retry

# ======== CONFIG ========
BASE = "https://api.sofascore.com/api/v1"
TIMEOUT = 30

HEADERS_PRIMARY = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/119.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}
HEADERS_FALLBACK = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}

# ======== ALIASES (apelidos -> nomes aceitos pelo ScraperFC) ========
SCRAPERFC_ALIASES = {
    # internacionais/clubes
    "champions league": "Champions League",
    "uefa champions league": "Champions League",
    "ucl": "Champions League",
    "europa league": "Europa League",
    "uefa europa league": "Europa League",
    "europa conference league": "Europa Conference League",

    # ligas nacionais principais
    "epl": "EPL",
    "premier league": "EPL",
    "english premier league": "EPL",
    "la liga": "La Liga",
    "laliga": "La Liga",
    "bundesliga": "Bundesliga",
    "serie a": "Serie A",
    "ligue 1": "Ligue 1",
    "turkish super lig": "Turkish Super Lig",
    "super lig": "Turkish Super Lig",
    "superliga turca": "Turkish Super Lig",

    # américas
    "mls": "MLS",
    "usl championship": "USL Championship",
    "usl1": "USL1",
    "usl 1": "USL1",
    "usl2": "USL2",
    "usl 2": "USL2",
    "argentina liga profesional": "Argentina Liga Profesional",
    "liga profesional argentina": "Argentina Liga Profesional",
    "argentina copa de la liga profesional": "Argentina Copa de la Liga Profesional",
    "copa de la liga": "Argentina Copa de la Liga Profesional",
    "liga 1 peru": "Liga 1 Peru",

    # arábia
    "saudi pro league": "Saudi Pro League",

    # seleções
    "world cup": "World Cup",
    "copa do mundo": "World Cup",
    "euros": "Euros",
    "euro": "Euros",
    "gold cup": "Gold Cup",
    "copa ouro": "Gold Cup",

    # libertadores e variantes
    "libertadores": "Copa Libertadores",
    "copa libertadores": "Copa Libertadores",

    # brasil - apelidos (Série A/B e Copa do Brasil)
    "brasileirao": "Brasileirão Série A",
    "campeonato brasileiro": "Brasileirão Série A",
    "serie a brasil": "Brasileirão Série A",
    "brasileirao serie a": "Brasileirão Série A",
    "brasileirao a": "Brasileirão Série A",

    "brasileirao b": "Brasileirão Série B",
    "campeonato brasileiro serie b": "Brasileirão Série B",
    "serie b brasil": "Brasileirão Série B",
    "brasileirao serie b": "Brasileirão Série B",

    "copa do brasil": "Copa do Brasil",
}

# ======== LISTA FALLBACK DE LIGAS (se ScraperFC não expuser listagem) ========
SCRAPERFC_VALID_LEAGUES_FALLBACK: List[str] = sorted(set(list(SCRAPERFC_ALIASES.values()) + [
    # adicionar outros nomes comuns suportados por versões recentes
    "Argentina Liga Profesional",
    "Argentina Copa de la Liga Profesional",
    "Liga 1 Peru",
    "Saudi Pro League",
    "USL Championship",
    "USL1",
    "USL2",
    "World Cup",
    "Euros",
    "Gold Cup",
    "Europa Conference League",
    "Champions League",
    "Europa League",
    "EPL",
    "La Liga",
    "Bundesliga",
    "Serie A",
    "Ligue 1",
    "Turkish Super Lig",
    "Copa Libertadores",
    "MLS",
    # Brasil (se sua versão do ScraperFC suportar)
    "Brasileirão Série A",
    "Brasileirão Série B",
    "Copa do Brasil",
]))

def normalize_league_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    key = s.strip().lower()
    return SCRAPERFC_ALIASES.get(key, s.strip())

# ======== UTILS ========
def extract_ids_from_url(url: str) -> Tuple[int, int]:
    """
    Extrai tournamentId e seasonId da URL do Sofascore.
    Ex.: .../conmebol-libertadores/384#id:70083 -> (384, 70083)
    """
    m_t = re.search(r"/(\d+)(?:[#?/]|$)", url)        # último número do path
    m_s = re.search(r"#id:(\d+)", url)                # seasonId no hash
    if not m_t or not m_s:
        raise ValueError("Não foi possível extrair tournamentId e seasonId da URL.")
    return int(m_t.group(1)), int(m_s.group(1))

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET'])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def get_json(session: requests.Session, url: str) -> dict:
    # tenta com headers "fortes" e depois fallback
    last_exc = None
    for headers in (HEADERS_PRIMARY, HEADERS_FALLBACK):
        r = session.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 403:
            time.sleep(0.6)
            last_exc = requests.HTTPError(f"403 em {url}")
            continue
        r.raise_for_status()
        return r.json()
    if last_exc:
        raise last_exc
    raise RuntimeError("Falha inesperada em get_json")

# ======== API DIRETA (INFO/STANDINGS/TEAMS/EVENTS) ========
def api_get_tournament_info(session: requests.Session, tournament_id: int) -> dict:
    url = f"{BASE}/unique-tournament/{tournament_id}"
    try:
        return get_json(session, url) or {}
    except Exception:
        return {}

def api_get_standings(session: requests.Session, tournament_id: int, season_id: int) -> pd.DataFrame:
    url = f"{BASE}/unique-tournament/{tournament_id}/season/{season_id}/standings"
    data = get_json(session, url)
    blocks = data.get("standings", []) or []
    rows = []
    for block in blocks:
        grupo = block.get("name") or block.get("type")
        for row in block.get("rows", []) or []:
            team = row.get("team", {}) or {}
            rows.append({
                "Grupo/Fase": grupo,
                "Pos": row.get("position"),
                "Time": team.get("name"),
                "TeamId": team.get("id"),
                "Jogos": row.get("matches"),
                "V": row.get("wins"),
                "E": row.get("draws"),
                "D": row.get("losses"),
                "GP": row.get("scoresFor") or row.get("goalsFor"),
                "GC": row.get("scoresAgainst") or row.get("goalsAgainst"),
                "SG": row.get("scoreDiff") or row.get("goalDiff"),
                "Pontos": row.get("points"),
            })
    return pd.DataFrame(rows)

def api_get_teams(session: requests.Session, tournament_id: int, season_id: int) -> pd.DataFrame:
    url = f"{BASE}/unique-tournament/{tournament_id}/season/{season_id}/teams"
    data = get_json(session, url)
    teams = data.get("teams", []) or []
    rows = []
    for t in teams:
        country = t.get("country") or {}
        rows.append({
            "TeamId": t.get("id"),
            "Nome": t.get("name"),
            "Slug": t.get("slug"),
            "Pais": country.get("name"),
            "PaisCode": country.get("alpha2"),
            "Cidade": t.get("city"),
            "Fundacao": t.get("founded"),
        })
    return pd.DataFrame(rows)

def api_get_events(session: requests.Session, tournament_id: int, season_id: int) -> pd.DataFrame:
    url = f"{BASE}/unique-tournament/{tournament_id}/season/{season_id}/events"
    try:
        data = get_json(session, url)
    except requests.HTTPError:
        return pd.DataFrame()
    events = data.get("events", []) or []
    rows = []
    for e in events:
        home = e.get("homeTeam", {}) or {}
        away = e.get("awayTeam", {}) or {}
        status = e.get("status", {}) or {}
        rows.append({
            "EventId": e.get("id"),
            "Rodada": (e.get("roundInfo") or {}).get("round"),
            "DataUTC_ts": e.get("startTimestamp"),
            "StatusType": status.get("type"),
            "StatusDesc": status.get("description"),
            "HomeTeam": home.get("name"),
            "HomeId": home.get("id"),
            "AwayTeam": away.get("name"),
            "AwayId": away.get("id"),
            "PlacarHome": (e.get("homeScore") or {}).get("current"),
            "PlacarAway": (e.get("awayScore") or {}).get("current"),
        })
    df = pd.DataFrame(rows)
    if not df.empty and "DataUTC_ts" in df.columns:
        df["DataUTC"] = pd.to_datetime(df["DataUTC_ts"], unit="s", utc=True)
        df.sort_values(by="DataUTC", inplace=True)
    return df

# ======== SCRAPERFC HELPERS ========
def list_scraperfc_leagues() -> List[str]:
    """
    Tenta obter a lista de ligas diretamente do pacote ScraperFC.
    Se não houver método público pra isso, usa um fallback extensivo.
    """
    try:
        from ScraperFC import Sofascore
        sfc = Sofascore()
        if hasattr(sfc, "get_valid_leagues"):
            leagues = sfc.get_valid_leagues()
            if isinstance(leagues, (list, tuple, set)):
                return sorted(set(leagues))
        return SCRAPERFC_VALID_LEAGUES_FALLBACK
    except ModuleNotFoundError:
        return SCRAPERFC_VALID_LEAGUES_FALLBACK

def fallback_scraperfc_matches_and_stats(league_name: str,
                                         season_id: Optional[int],
                                         year_override: Optional[int]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    league_name: nome aceito pelo ScraperFC (já normalizado)
    season_id : seasonId da URL do Sofascore (para tentar mapear pro 'ano')
    year_override: se informado, força este ano
    """
    from ScraperFC import Sofascore  # import tardio
    sfc = Sofascore()

    seasons = sfc.get_valid_seasons(league_name)  # ex.: {"2025": 70083, "2024": 6xxxx, ...}
    inv = {v: k for k, v in seasons.items()}      # seasonId->ano (string)

    if year_override:
        year_str = str(year_override)
    else:
        year_str = inv.get(season_id) or (seasons and max(seasons.keys()))
    if not year_str:
        raise RuntimeError(f"Não foi possível deduzir o ano para a liga '{league_name}' no fallback.")

    matches = sfc.get_match_dicts(year=year_str, league=league_name)

    def get_in(d, *path):
        cur = d
        for k in path:
            cur = (cur or {}).get(k) if isinstance(cur, dict) else None
        return cur

    rows = []
    for m in matches:
        rows.append({
            "EventId": m.get("id"),
            "DataUTC_ts": m.get("startTimestamp"),
            "HomeTeam": get_in(m, "homeTeam", "name"),
            "HomeId": get_in(m, "homeTeam", "id"),
            "AwayTeam": get_in(m, "awayTeam", "name"),
            "AwayId": get_in(m, "awayTeam", "id"),
            "PlacarHome": get_in(m, "homeScore", "current"),
            "PlacarAway": get_in(m, "awayScore", "current"),
            "Rodada": get_in(m, "roundInfo", "round"),
            "StatusType": get_in(m, "status", "type"),
            "StatusDesc": get_in(m, "status", "description"),
        })
    df_matches = pd.DataFrame(rows)
    if not df_matches.empty and "DataUTC_ts" in df_matches.columns:
        df_matches["DataUTC"] = pd.to_datetime(df_matches["DataUTC_ts"], unit="s", utc=True)
        df_matches.sort_values(by="DataUTC", inplace=True)

    df_stats = sfc.scrape_player_league_stats(year=year_str, league=league_name, accumulation="total")
    return df_matches, df_stats

# ======== INTERAÇÃO NO CONSOLE ========
def _prompt_choose(options: List[str], title: str) -> int:
    print(f"\n{title}")
    for i, opt in enumerate(options, start=1):
        print(f"  {i:2d}. {opt}")
    while True:
        raw = input("Escolha um número: ").strip()
        if not raw.isdigit():
            print("Digite um número válido.")
            continue
        idx = int(raw)
        if 1 <= idx <= len(options):
            return idx - 1
        print("Opção fora do intervalo. Tente novamente.")

def run_interactive_latest_only() -> None:
    """
    Lista ligas, você escolhe 1, e ele consulta automaticamente a temporada MAIS RECENTE dessa liga.
    Gera: events_fallback.csv e player_stats_fallback.csv.
    """
    try:
        from ScraperFC import Sofascore  # valida instalação
    except ModuleNotFoundError:
        print("❌ ScraperFC não está instalado. Instale com: python -m pip install ScraperFC")
        return

    leagues = list_scraperfc_leagues()
    if not leagues:
        print("❌ Não foi possível obter a lista de ligas.")
        return

    li = _prompt_choose(leagues, "== Ligas disponíveis (ScraperFC) ==")
    league_name = leagues[li]
    print(f"➡️  Liga selecionada: {league_name}")

    from ScraperFC import Sofascore
    sfc = Sofascore()
    seasons = sfc.get_valid_seasons(league_name)  # dict { "2025": seasonId, "2024": ... }
    if not seasons:
        print("❌ Não há temporadas disponíveis para essa liga no ScraperFC.")
        return

    latest_year = max(seasons.keys())  # mais recente
    print(f"📅 Usando a temporada mais recente: {latest_year}")

    try:
        df_matches, df_stats = fallback_scraperfc_matches_and_stats(
            league_name=league_name,
            season_id=None,
            year_override=int(latest_year)
        )

        if not df_matches.empty:
            df_matches.to_csv("events_fallback.csv", index=False, encoding="utf-8-sig")
            print(f"✅ events_fallback.csv gerado (liga={league_name}, ano={latest_year}).")
        else:
            print("⚠️ Não foi possível coletar jogos no fallback.")

        if isinstance(df_stats, pd.DataFrame) and not df_stats.empty:
            df_stats.to_csv("player_stats_fallback.csv", index=False, encoding="utf-8-sig")
            print("✅ player_stats_fallback.csv gerado (via ScraperFC).")
        else:
            print("⚠️ Não foi possível coletar estatísticas de jogadores no fallback.")
    except Exception as e:
        print(f"❗ Erro no modo interativo/fallback: {e}")

# ======== MAIN ========
def main():
    parser = argparse.ArgumentParser(description="Coleta dados de ligas da Sofascore (API oficial + fallback ScraperFC).")
    parser.add_argument("--url", help="URL do torneio/temporada no Sofascore (ex.: .../384#id:70083)")
    parser.add_argument("--league", default=None, help="Nome da liga para o fallback (ex.: 'Copa Libertadores', 'EPL', 'La Liga').")
    parser.add_argument("--year", type=int, default=None, help="Ano para o fallback (ex.: 2025). Se omitido, tenta deduzir pelo seasonId.")
    parser.add_argument("--list-leagues", action="store_true", help="Lista ligas aceitas pelo ScraperFC e sai.")
    parser.add_argument("--list-aliases", action="store_true", help="Lista os aliases de ligas suportados para normalização e sai.")
    args = parser.parse_args()

    # ----- Somente listagens -----
    if args.list_leagues:
        leagues = list_scraperfc_leagues()
        print("✅ Ligas aceitas pelo ScraperFC (ordem alfabética):")
        for name in leagues:
            print(f"- {name}")
        return

    if args.list_aliases:
        print("✅ Aliases suportados (entrada -> nome normalizado):")
        for k in sorted(SCRAPERFC_ALIASES):
            print(f"- {k}  ->  {SCRAPERFC_ALIASES[k]}")
        return

    # ----- Execução normal -----
    if not args.url:
        # Sem argumentos de URL: entra no modo interativo (escolhe liga e usa a temporada mais recente)
        run_interactive_latest_only()
        return

    # Com URL: tenta API oficial, depois fallback
    liga_url = args.url
    tournament_id, season_id = extract_ids_from_url(liga_url)
    session = build_session()

    # Descobre nome da liga via API (para usar no fallback se usuário não passar)
    fallback_league = normalize_league_name(args.league) if args.league else None
    if not fallback_league:
        info = api_get_tournament_info(session, tournament_id)
        name = None
        try:
            name = (info.get("uniqueTournament") or {}).get("name")
        except Exception:
            name = None
        fallback_league = normalize_league_name(name)

    # --- Tenta API oficial ---
    try:
        df_stand = api_get_standings(session, tournament_id, season_id)
        df_teams = api_get_teams(session, tournament_id, season_id)
        df_events = api_get_events(session, tournament_id, season_id)

        if not df_stand.empty:
            df_stand.sort_values(by=["Grupo/Fase", "Pos"], inplace=True, kind="stable")
            df_stand.to_csv("standings.csv", index=False, encoding="utf-8-sig")
            print("✅ standings.csv gerado.")
        else:
            print("⚠️ Standings vazio.")

        if not df_teams.empty:
            df_teams.to_csv("teams.csv", index=False, encoding="utf-8-sig")
            print("✅ teams.csv gerado.")
        else:
            print("⚠️ Teams vazio.")

        if not df_events.empty:
            df_events.to_csv("events.csv", index=False, encoding="utf-8-sig")
            print("✅ events.csv gerado.")
        else:
            print("ℹ️ events.csv não gerado/indisponível.")
        return

    except requests.exceptions.RetryError as e:
        print(f"🔁 API bloqueou com muitos 403 (RetryError): {e}")
    except requests.HTTPError as e:
        print(f"⛔ API retornou erro: {e}")
    except Exception as e:
        print(f"❗ Erro inesperado na rota API: {e}")

    # --- Fallback via ScraperFC ---
    print("↩️  Caindo no fallback via ScraperFC (partidas + stats de jogadores)...")
    try:
        if not fallback_league:
            raise RuntimeError(
                "Não consegui deduzir automaticamente o nome da liga para o fallback. "
                "Use --league (ex.: --league 'Copa Libertadores', --league 'EPL'). "
                "Ou rode sem --url para escolher a liga no console."
            )

        df_matches, df_stats = fallback_scraperfc_matches_and_stats(
            league_name=fallback_league,
            season_id=season_id,
            year_override=args.year
        )

        if not df_matches.empty:
            df_matches.to_csv("events_fallback.csv", index=False, encoding="utf-8-sig")
            print(f"✅ events_fallback.csv gerado (via ScraperFC, liga={fallback_league}).")
        else:
            print("⚠️ Não foi possível coletar jogos no fallback.")

        if isinstance(df_stats, pd.DataFrame) and not df_stats.empty:
            df_stats.to_csv("player_stats_fallback.csv", index=False, encoding="utf-8-sig")
            print("✅ player_stats_fallback.csv gerado (via ScraperFC).")
        else:
            print("⚠️ Não foi possível coletar estatísticas de jogadores no fallback.")

    except ModuleNotFoundError:
        print("❌ ScraperFC não está instalado. Instale com: python -m pip install ScraperFC")
    except Exception as e:
        print(f"❗ Erro no fallback ScraperFC: {e}")


if __name__ == "__main__":
    main()
