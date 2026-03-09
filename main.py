"""
=============================================================
  DETECTOR DE ANÚNCIOS DE RÁDIO
  Versão 2.1 — Fila centralizada (sem segfault)
=============================================================

  ╔══════════════════════════════════════════════╗
  ║   CONFIGURE AS RÁDIOS AQUI (adicione/remova) ║
  ╚══════════════════════════════════════════════╝

  Formato: "Nome_da_Radio": "URL_do_stream"
  Para desativar uma rádio sem apagar, comente a linha com #
"""

STATIONS = {
    "Band_FM":      "https://stm.alphanetdigital.com.br:7040/band",
    "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream",
    "Jovem_Pan": "https://sc1s.cdn.upx.com:9986/stream?1772563648730",
    # "Radio_exemplo": "https://url-do-stream-aqui",
}

# ─── Configurações gerais ────────────────────────────────────────────────────

RECORD_DURATION      = 30       # segundos por captura
SLEEP_BETWEEN_CYCLES = 5        # segundos entre ciclos completos
WHISPER_MODEL        = "large"  # tiny | base | small | medium | large
OLLAMA_URL           = "http://localhost:11434/api/generate"
OLLAMA_MODEL         = "gemma3:4b"
MIN_SPEECH_RATIO     = 0.30     # fração mínima de fala para processar
MIN_SPEECH_SEGS      = 2        # mínimo de segmentos de fala
TRANSCRIPTION_CAP    = 1200     # máx de chars enviados ao LLM

# ─── Imports ─────────────────────────────────────────────────────────────────

import subprocess, datetime, os, re, time, shutil, json, requests, unicodedata
import threading, queue, traceback
from zoneinfo import ZoneInfo

import numpy as np
import librosa
import torch
import whisper

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─── Timezone ────────────────────────────────────────────────────────────────

try:
    TZ_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_BR = None


def br_now():
    return datetime.datetime.now(TZ_BR) if TZ_BR else datetime.datetime.now()


def br_timestamp():
    return br_now().strftime("%d-%m-%Y_%H-%M-%S")


def br_display():
    return br_now().strftime("%d/%m/%Y %H:%M:%S")


# ─── Utilitários ─────────────────────────────────────────────────────────────

def safe_filename(text: str, max_len: int = 60) -> str:
    if not text:
        return "Desconhecido"
    text = str(text).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\- ]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text).strip("_")
    return (text or "Desconhecido")[:max_len]


# ─── Heurística ──────────────────────────────────────────────────────────────

STRONG_AD_KEYWORDS = [
    "promoção", "oferta", "desconto", "imperdível", "compre", "aproveite",
    "garanta", "só hoje", "últimos dias", "parcel", "sem juros", "frete",
    "cupom", "código", "whatsapp", "zap", "ligue", "telefone", "disque",
    "site", ".com", ".br", "instagram", "@", "delivery", "preço", "reais",
    "r$", "por apenas", "a partir de", "vem pra", "venha", "visite",
    "confira", "peça já", "acesse", "baixe", "clique",
]

BUSINESS_WORDS = [
    "farmácia", "drogaria", "autoescola", "mercado", "supermercado",
    "clínica", "laboratório", "ótica", "loja", "móveis", "colchões",
    "academia", "concessionária", "pizzaria", "lanchonete", "restaurante",
    "pet shop", "seguro", "consórcio", "financiamento", "imobiliária",
    "construtora", "hospital", "quilo", "posto", "oficina", "hotel",
    "pousada", "colégio", "faculdade", "curso", "clique", "aplicativo",
]

NON_AD_PHRASES = [
    "segundo informações", "de acordo com", "o governador", "o prefeito",
    "a prefeitura", "a polícia", "os bombeiros", "o governo", "a câmara",
    "o congresso", "o presidente", "a secretaria", "a temperatura",
    "boa tarde", "bom dia", "boa noite", "você está ouvindo",
    "próxima música", "agora vamos", "fique ligado",
]


def heuristic_score(text: str) -> dict:
    if not text:
        return {"ad_score": 0, "nonad_score": 0, "has_price": False, "has_phone": False}
    t      = text.lower()
    price  = bool(re.search(r"r\$\s*\d+([.,]\d{2})?|\d+\s*reais", t))
    phone  = bool(re.search(r"(\(?\d{2}\)?\s*)?\d{4,5}[-\s]?\d{4}", t))
    strong = sum(1 for k in STRONG_AD_KEYWORDS if k in t)
    biz    = sum(1 for k in BUSINESS_WORDS if k in t)
    nonad  = sum(1 for k in NON_AD_PHRASES  if k in t)
    ad_score = strong * 2 + biz + (3 if price else 0) + (2 if phone else 0)
    return {"ad_score": ad_score, "nonad_score": nonad, "has_price": price, "has_phone": phone}


# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER DE GRAVAÇÃO — roda em thread separada por rádio
#  Apenas grava o áudio e empurra o caminho do arquivo para a fila central
# ═══════════════════════════════════════════════════════════════════════════════

def recorder_worker(name: str, url: str, audio_path: str,
                    work_queue: queue.Queue, stop_event: threading.Event):
    print(f"  🎙️  Gravador iniciado: {name}")
    while not stop_event.is_set():
        ts        = br_timestamp()
        file_path = os.path.join(audio_path, f"{safe_filename(name)}_{ts}.mp3")
        cmd = [
            "ffmpeg", "-i", url,
            "-t", str(RECORD_DURATION),
            "-acodec", "libmp3lame",
            "-ar", "16000", "-ac", "1",
            file_path, "-y", "-loglevel", "quiet",
        ]
        try:
            subprocess.run(cmd, check=True, timeout=RECORD_DURATION + 15)
            if os.path.exists(file_path):
                work_queue.put((name, file_path))
        except Exception as e:
            print(f"  ⚠️  [{name}] Erro na gravação: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)

        time.sleep(2)

    print(f"  🛑 Gravador encerrado: {name}")


# ═══════════════════════════════════════════════════════════════════════════════
#  CLASSE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

class AdDetector:
    def __init__(self):
        self.base_path   = "radio_capture"
        self.audio_path  = os.path.join(self.base_path, "temp_audios")
        self.log_path    = os.path.join(self.base_path, "logs")
        self.ads_path    = os.path.join(self.base_path, "detected_ads")
        self.report_path = os.path.join(self.base_path, "relatorio_anuncios.xlsx")

        for folder in [self.audio_path, self.log_path, self.ads_path]:
            os.makedirs(folder, exist_ok=True)

        print("🔧 Carregando Silero VAD...")
        self.vad_model, utils = torch.hub.load(
            repo_or_dir="snakers4/silero-vad",
            model="silero_vad",
            trust_repo=True,
        )
        self.get_speech_timestamps = utils[0]

        print(f"🔧 Carregando Whisper ({WHISPER_MODEL})...")
        self.whisper_model = whisper.load_model(WHISPER_MODEL)

        self._init_excel()
        print("✅ Pronto.\n")

    # ── Excel ────────────────────────────────────────────────────────────────

    def _init_excel(self):
        if os.path.exists(self.report_path):
            print(f"📊 Relatório existente: {self.report_path}")
            return

        wb = Workbook()
        ws = wb.active
        ws.title = "Anúncios Detectados"

        headers = [
            "Data/Hora", "Rádio", "Anunciante", "Produto/Serviço",
            "Confiança", "Transcrição (resumo)", "Arquivo de Áudio",
        ]

        header_fill = PatternFill("solid", start_color="1F4E79")
        header_font = Font(bold=True, color="FFFFFF", name="Arial", size=11)
        thin_side   = Side(style="thin", color="CCCCCC")
        border      = Border(left=thin_side, right=thin_side, bottom=thin_side)

        for col, h in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font      = header_font
            cell.fill      = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border    = border

        ws.row_dimensions[1].height = 30
        ws.freeze_panes = "A2"

        for i, w in enumerate([20, 15, 22, 22, 12, 60, 40], start=1):
            ws.column_dimensions[get_column_letter(i)].width = w

        ws2 = wb.create_sheet("Resumo por Rádio")
        for col, h in enumerate(["Rádio", "Total de Anúncios", "Última Detecção"], start=1):
            cell = ws2.cell(row=1, column=col, value=h)
            cell.font = Font(bold=True, color="FFFFFF", name="Arial")
            cell.fill = PatternFill("solid", start_color="2E75B6")

        wb.save(self.report_path)
        print(f"📊 Relatório criado: {self.report_path}")

    def _append_to_excel(self, station: str, info: dict, snippet: str, audio_file: str):
        try:
            wb = load_workbook(self.report_path)
            ws = wb["Anúncios Detectados"]
            next_row = ws.max_row + 1

            conf       = info.get("confianca", "baixa")
            fill_color = {"alta": "E2EFDA", "media": "FFF2CC", "baixa": "FCE4D6"}.get(conf, "FFFFFF")
            row_fill   = PatternFill("solid", start_color=fill_color)
            row_font   = Font(name="Arial", size=10)
            thin_side  = Side(style="thin", color="DDDDDD")
            row_border = Border(left=thin_side, right=thin_side, bottom=thin_side)

            valores = [
                br_display(),
                station,
                info.get("anunciante") or "—",
                info.get("produto")    or "—",
                conf.upper(),
                (snippet or "")[:200],
                os.path.basename(audio_file),
            ]

            for col, val in enumerate(valores, start=1):
                cell = ws.cell(row=next_row, column=col, value=val)
                cell.font      = row_font
                cell.fill      = row_fill
                cell.border    = row_border
                cell.alignment = Alignment(vertical="center", wrap_text=(col == 6))
            ws.row_dimensions[next_row].height = 18

            ws2 = wb["Resumo por Rádio"]
            station_row = None
            for row in ws2.iter_rows(min_row=2):
                if row[0].value == station:
                    station_row = row
                    break

            if station_row:
                station_row[1].value = (station_row[1].value or 0) + 1
                station_row[2].value = br_display()
            else:
                nr = ws2.max_row + 1
                ws2.cell(row=nr, column=1, value=station)
                ws2.cell(row=nr, column=2, value=1)
                ws2.cell(row=nr, column=3, value=br_display())

            wb.save(self.report_path)
            print(f"  ✅ Excel salvo ({next_row - 1} anúncios no total)")

        except Exception as e:
            print(f"  ⚠️  Erro ao salvar Excel: {e}")
            traceback.print_exc()

    # ── VAD ──────────────────────────────────────────────────────────────────

    def analyze_vad(self, file_path: str):
        try:
            y, sr = librosa.load(file_path, sr=16000)
            wav   = torch.from_numpy(y).float()
            segs  = self.get_speech_timestamps(wav, self.vad_model, sampling_rate=16000)

            if not segs:
                return None

            duration     = len(y) / sr
            total_speech = sum((s["end"] - s["start"]) / sr for s in segs)
            ratio        = total_speech / duration

            if ratio < MIN_SPEECH_RATIO or len(segs) < MIN_SPEECH_SEGS:
                return None

            return {"speech_ratio": ratio, "fragments": len(segs)}
        except Exception as e:
            print(f"  ⚠️  Erro VAD: {e}")
            return None

    # ── Whisper ──────────────────────────────────────────────────────────────

    def transcribe(self, file_path: str) -> str:
        try:
            result = self.whisper_model.transcribe(
                file_path,
                language="pt",
                fp16=False,
                temperature=0,
                condition_on_previous_text=False,
            )
            return (result.get("text") or "").strip()
        except Exception as e:
            print(f"  ⚠️  Erro Whisper: {e}")
            traceback.print_exc()
            return ""

    # ── LLM ──────────────────────────────────────────────────────────────────

    def _ollama_classify(self, transcription: str, heur: dict) -> dict:
        dica = ""
        if heur["ad_score"] >= 4:
            dica = "ATENÇÃO: análise automática sugere fortemente que é um anúncio."
        elif heur["ad_score"] >= 2:
            dica = "Análise automática detectou alguns indicadores de anúncio."
        elif heur["nonad_score"] >= 2:
            dica = "Análise automática sugere conteúdo jornalístico/informativo."

        prompt = f"""Você é um classificador especializado em áudio de rádio brasileiro.
Analise o texto transcrito e decida se é um ANÚNCIO PUBLICITÁRIO ou CONTEÚDO NORMAL.

Critérios para ANÚNCIO:
- Promoções, preços, descontos, parcelamentos
- Nome de empresa/marca vendendo produto ou serviço
- Call-to-action: compre, visite, ligue, acesse, aproveite
- Endereço, telefone, site, Instagram, WhatsApp de negócio

Critérios para NÃO-ANÚNCIO:
- Locução informativa (notícias, boletins, previsão do tempo)
- Apresentação de músicas ou programas
- Conversa entre locutores sem venda

{dica}

Texto:
\"\"\"{transcription}\"\"\"

Responda SOMENTE com JSON válido:
{{
  "eh_anuncio": true ou false,
  "anunciante": "nome da marca" ou null,
  "produto": "produto/serviço" ou null,
  "confianca": "alta" ou "media" ou "baixa",
  "motivo_curto": "justificativa em 1 frase"
}}""".strip()

        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False,
                    "options": {"temperature": 0},
                },
                timeout=120,
            )
            raw  = resp.json().get("response", "")
            data = raw if isinstance(raw, dict) else {}
            if not data:
                raw = (raw or "").strip()
                try:
                    data = json.loads(raw)
                except Exception:
                    m = re.search(r"\{.*\}", raw, flags=re.S)
                    data = json.loads(m.group(0)) if m else {}

            conf = str(data.get("confianca", "baixa")).lower().strip()
            data["confianca"] = conf if conf in ("alta", "media", "baixa") else "baixa"

            for k in ("anunciante", "produto"):
                v = data.get(k)
                if isinstance(v, str) and v.strip().lower() in ("", "null", "none"):
                    data[k] = None

            data["eh_anuncio"] = bool(data.get("eh_anuncio", False))
            return data

        except Exception as e:
            print(f"  ⚠️  Erro Ollama: {e}")
            return {"eh_anuncio": False, "confianca": "baixa", "anunciante": None, "produto": None}

    def classify(self, transcription: str) -> dict:
        transcription = (transcription or "").strip()
        empty = {"eh_anuncio": False, "confianca": "baixa", "anunciante": None, "produto": None}

        if len(transcription) < 25:
            return empty

        heur = heuristic_score(transcription)
        data = self._ollama_classify(transcription, heur)

        eh         = bool(data.get("eh_anuncio", False))
        conf       = data.get("confianca", "baixa")
        anunciante = data.get("anunciante")

        if eh and conf == "alta":
            return data
        if eh and conf == "media" and heur["ad_score"] >= 2:
            return data
        if eh and conf == "baixa" and heur["ad_score"] >= 5 and anunciante:
            data["confianca"] = "media"
            return data
        if not eh and heur["ad_score"] >= 6 and heur["has_price"]:
            data["eh_anuncio"] = True
            data["confianca"]  = "baixa"
            return data
        if heur["nonad_score"] >= 2 and conf != "alta":
            return empty

        return empty

    # ── Salvar áudio ─────────────────────────────────────────────────────────

    def save_ad(self, station: str, audio_file: str, info: dict) -> str:
        marca   = safe_filename(info.get("anunciante") or "Desconhecido")
        produto = safe_filename(info.get("produto")    or "")
        ts      = br_timestamp()
        parts   = [safe_filename(station), marca]
        if produto and produto != "Desconhecido":
            parts.append(produto)
        parts.append(ts)
        dest = os.path.join(self.ads_path, "__".join(parts) + ".mp3")
        shutil.copy2(audio_file, dest)
        return dest

    # ── Processar item da fila ────────────────────────────────────────────────

    def process_item(self, name: str, audio_file: str):
        """Sempre chamado na thread principal — sem paralelismo no Whisper/VAD."""
        try:
            vad = self.analyze_vad(audio_file)
            if not vad:
                print(f"  🎵 [{name}] Ignorado (pouca fala)")
                return

            print(f"  🔍 [{name}] Transcrevendo... (speech={vad['speech_ratio']:.0%}, frags={vad['fragments']})")
            text    = self.transcribe(audio_file)
            snippet = text[:TRANSCRIPTION_CAP]

            if not snippet:
                print(f"  ⚠️  [{name}] Transcrição vazia, ignorando.")
                return

            print(f"  📝 [{name}] {snippet[:200].replace(chr(10), ' ')!r}")

            info = self.classify(snippet)

            if info.get("eh_anuncio"):
                marca = info.get("anunciante") or "Desconhecido"
                conf  = info.get("confianca", "media")
                print(f"  📢 [{name}] ANÚNCIO → {marca} (conf={conf})")
                saved = self.save_ad(name, audio_file, info)
                self._append_to_excel(name, info, snippet, saved)
                print(f"  💾 [{name}] Áudio: {os.path.basename(saved)}")
            else:
                motivo = info.get("motivo_curto", "")
                print(f"  🎵 [{name}] Não é anúncio. {motivo}")

        except Exception as e:
            print(f"  ❌ [{name}] Erro: {e}")
            traceback.print_exc()
        finally:
            # Garante limpeza do arquivo temporário sempre
            if os.path.exists(audio_file):
                os.remove(audio_file)

    # ── Loop principal ────────────────────────────────────────────────────────

    def run(self):
        print("🚀 Iniciando monitoramento contínuo...")
        print(f"   Rádios: {', '.join(STATIONS.keys())}")
        print(f"   Relatório: {os.path.abspath(self.report_path)}")
        print("   (Ctrl+C para parar)\n")

        work_queue = queue.Queue()
        stop_event = threading.Event()
        threads    = []

        # Uma thread de gravação por rádio (só grava, não processa)
        for name, url in STATIONS.items():
            t = threading.Thread(
                target=recorder_worker,
                args=(name, url, self.audio_path, work_queue, stop_event),
                daemon=True,
                name=f"rec-{name}",
            )
            t.start()
            threads.append(t)

        print(f"  🎙️  {len(threads)} gravadores iniciados.\n")

        try:
            while True:
                try:
                    # Bloqueia até chegar um arquivo, com timeout para checar Ctrl+C
                    name, audio_file = work_queue.get(timeout=2)
                    print(f"\n{'─'*60}")
                    print(f"📥 [{name}] Novo áudio — {br_display()}")
                    self.process_item(name, audio_file)
                    work_queue.task_done()
                except queue.Empty:
                    continue

        except KeyboardInterrupt:
            print("\n\n🛑 Encerrando...")
            stop_event.set()
            for t in threads:
                t.join(timeout=5)
            print("👋 Encerrado.")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    detector = AdDetector()
    detector.run()