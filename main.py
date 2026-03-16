"""
=============================================================
  DETECTOR DE ANÚNCIOS DE RÁDIO
  Versão 3.1 — Groq Cloud + múltiplos anúncios por ciclo
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
    #"Jovem_Pan":    "https://sc1s.cdn.upx.com:9986/stream?1772563648730",
    # "Radio_exemplo": "https://url-do-stream-aqui",
}

# ─── Configurações gerais ────────────────────────────────────────────────────

RECORD_DURATION      = 30       # segundos por captura
GROQ_WHISPER_MODEL   = "whisper-large-v3"
GROQ_LLM_MODEL       = "llama-3.3-70b-versatile"
MIN_SPEECH_RATIO     = 0.30     # fração mínima de fala para processar
MIN_SPEECH_SEGS      = 2        # mínimo de segmentos de fala
TRANSCRIPTION_CAP    = 3000     # máx de chars enviados ao LLM (aumentado para 30s)

# ─── Imports ─────────────────────────────────────────────────────────────────

import subprocess, datetime, os, re, time, shutil, json, unicodedata
import threading, queue, traceback
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

import librosa
import torch

from groq import Groq

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─── Carregar variáveis de ambiente ──────────────────────────────────────────

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY não encontrada! Verifique o arquivo .env")

groq_client = Groq(api_key=GROQ_API_KEY)

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
#  WORKER DE GRAVAÇÃO 
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

        print("☁️  Groq configurado:")
        print(f"     Transcrição : {GROQ_WHISPER_MODEL}")
        print(f"     LLM         : {GROQ_LLM_MODEL}")
        print(f"     Ciclo       : {RECORD_DURATION}s")

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

    # ── Transcrição via Groq Whisper ──────────────────────────────────────────

    def transcribe(self, file_path: str) -> str:
        try:
            with open(file_path, "rb") as f:
                response = groq_client.audio.transcriptions.create(
                    file=(os.path.basename(file_path), f),
                    model=GROQ_WHISPER_MODEL,
                    language="pt",
                    response_format="text",
                )
            return (response if isinstance(response, str) else str(response)).strip()
        except Exception as e:
            print(f"  ⚠️  Erro Groq Whisper: {e}")
            traceback.print_exc()
            return ""

    # ── Classificação múltipla via Groq LLM ──────────────────────────────────

    def _groq_classify_multi(self, transcription: str, heur: dict) -> list:
        dica = ""
        if heur["ad_score"] >= 4:
            dica = "ATENÇÃO: análise automática sugere fortemente que há anúncios."
        elif heur["ad_score"] >= 2:
            dica = "Análise automática detectou alguns indicadores de anúncio."
        elif heur["nonad_score"] >= 2:
            dica = "Análise automática sugere conteúdo predominantemente jornalístico/informativo."

        prompt = f"""Você é um classificador especializado em áudio de rádio brasileiro.
O texto abaixo é a transcrição de {RECORD_DURATION} segundos de rádio e pode conter ZERO, UM ou MAIS anúncios publicitários diferentes, intercalados com músicas, notícias ou locuções normais.

Sua tarefa:
1. Identifique CADA anúncio publicitário distinto presente no texto.
2. Anúncios diferentes têm anunciantes/marcas diferentes — não agrupe dois anunciantes em um só.
3. Se não houver nenhum anúncio, retorne lista vazia.

Critérios para ANÚNCIO:
- Promoções, preços, descontos, parcelamentos
- Nome de empresa/marca vendendo produto ou serviço
- Call-to-action: compre, visite, ligue, acesse, aproveite
- Endereço, telefone, site, Instagram, WhatsApp de negócio

Critérios para NÃO-ANÚNCIO (ignore esses trechos):
- Locução informativa (notícias, boletins, previsão do tempo)
- Apresentação de músicas ou programas
- Conversa entre locutores sem venda
- Letras de música

{dica}

Texto transcrito:
\"\"\"{transcription}\"\"\"

Responda SOMENTE com JSON válido no formato:
{{
  "anuncios": [
    {{
      "anunciante": "nome da marca/empresa",
      "produto": "produto ou serviço anunciado",
      "confianca": "alta" ou "media" ou "baixa",
      "motivo_curto": "justificativa em 1 frase",
      "trecho": "trecho resumido do texto que originou essa detecção (máx 100 chars)"
    }}
  ]
}}

Se não houver anúncios: {{"anuncios": []}}""".strip()

        try:
            response = groq_client.chat.completions.create(
                model=GROQ_LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=800,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content or ""
            data = {}
            try:
                data = json.loads(raw)
            except Exception:
                m = re.search(r"\{.*\}", raw, flags=re.S)
                data = json.loads(m.group(0)) if m else {}

            anuncios = data.get("anuncios", [])
            if not isinstance(anuncios, list):
                return []

            resultado = []
            seen_anunciantes = set()

            for ad in anuncios:
                if not isinstance(ad, dict):
                    continue

                anunciante = ad.get("anunciante") or ""
                if isinstance(anunciante, str) and anunciante.strip().lower() in ("", "null", "none"):
                    anunciante = None

                produto = ad.get("produto") or ""
                if isinstance(produto, str) and produto.strip().lower() in ("", "null", "none"):
                    produto = None

                conf = str(ad.get("confianca", "baixa")).lower().strip()
                if conf not in ("alta", "media", "baixa"):
                    conf = "baixa"

                # Deduplicar pelo nome do anunciante (case-insensitive)
                chave = (anunciante or "").strip().lower()
                if chave and chave in seen_anunciantes:
                    continue
                if chave:
                    seen_anunciantes.add(chave)

                resultado.append({
                    "eh_anuncio":   True,
                    "anunciante":   anunciante,
                    "produto":      produto,
                    "confianca":    conf,
                    "motivo_curto": ad.get("motivo_curto", ""),
                    "trecho":       ad.get("trecho", ""),
                })

            return resultado

        except Exception as e:
            print(f"  ⚠️  Erro Groq LLM: {e}")
            return []

    def classify_multi(self, transcription: str) -> list:
        transcription = (transcription or "").strip()
        if len(transcription) < 25:
            return []

        heur     = heuristic_score(transcription)
        anuncios = self._groq_classify_multi(transcription, heur)

        aprovados = []
        for ad in anuncios:
            conf       = ad.get("confianca", "baixa")
            anunciante = ad.get("anunciante")

            if conf == "alta":
                aprovados.append(ad)
            elif conf == "media" and heur["ad_score"] >= 2:
                aprovados.append(ad)
            elif conf == "baixa" and heur["ad_score"] >= 5 and anunciante:
                ad["confianca"] = "media"
                aprovados.append(ad)

        # Fallback heurístico se o LLM não detectou nada mas score é muito alto
        if not aprovados and heur["ad_score"] >= 6 and heur["has_price"]:
            aprovados.append({
                "eh_anuncio":   True,
                "anunciante":   None,
                "produto":      None,
                "confianca":    "baixa",
                "motivo_curto": "Detectado por heurística (preço + palavras-chave)",
                "trecho":       "",
            })

        return aprovados

    # ── Salvar áudio ─────────────────────────────────────────────────────────

    def save_ad(self, station: str, audio_file: str, info: dict, index: int = 0) -> str:
        marca   = safe_filename(info.get("anunciante") or "Desconhecido")
        produto = safe_filename(info.get("produto")    or "")
        ts      = br_timestamp()
        parts   = [safe_filename(station), marca]
        if produto and produto != "Desconhecido":
            parts.append(produto)
        parts.append(ts)
        if index > 0:
            parts.append(f"ad{index}")
        dest = os.path.join(self.ads_path, "__".join(parts) + ".mp3")
        shutil.copy2(audio_file, dest)
        return dest

    # ── Processar item da fila ────────────────────────────────────────────────

    def process_item(self, name: str, audio_file: str):
        try:
            vad = self.analyze_vad(audio_file)
            if not vad:
                print(f"  🎵 [{name}] Ignorado (pouca fala)")
                return

            print(f"  🔍 [{name}] Transcrevendo via Groq... (speech={vad['speech_ratio']:.0%}, frags={vad['fragments']})")
            text    = self.transcribe(audio_file)
            snippet = text[:TRANSCRIPTION_CAP]

            if not snippet:
                print(f"  ⚠️  [{name}] Transcrição vazia, ignorando.")
                return

            print(f"  📝 [{name}] {snippet[:200].replace(chr(10), ' ')!r}")

            anuncios = self.classify_multi(snippet)

            if not anuncios:
                print(f"  🎵 [{name}] Nenhum anúncio detectado.")
                return

            print(f"  📢 [{name}] {len(anuncios)} anúncio(s) detectado(s) neste ciclo!")

            for i, info in enumerate(anuncios, start=1):
                marca  = info.get("anunciante") or "Desconhecido"
                conf   = info.get("confianca", "media")
                trecho = info.get("trecho", "")
                print(f"       [{i}] {marca} (conf={conf}) — {trecho[:80]}")

                idx   = i if len(anuncios) > 1 else 0
                saved = self.save_ad(name, audio_file, info, index=idx)
                self._append_to_excel(name, info, snippet, saved)
                print(f"       💾 Áudio: {os.path.basename(saved)}")

        except Exception as e:
            print(f"  ❌ [{name}] Erro: {e}")
            traceback.print_exc()
        finally:
            if os.path.exists(audio_file):
                os.remove(audio_file)

    # ── Loop principal ────────────────────────────────────────────────────────

    def run(self):
        print("🚀 Iniciando monitoramento contínuo...")
        print(f"   Rádios   : {', '.join(STATIONS.keys())}")
        print(f"   Duração  : {RECORD_DURATION}s por ciclo")
        print(f"   Relatório: {os.path.abspath(self.report_path)}")
        print("   (Ctrl+C para parar)\n")

        work_queue = queue.Queue()
        stop_event = threading.Event()
        threads    = []

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