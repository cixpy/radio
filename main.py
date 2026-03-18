"""
=============================================================
  DETECTOR DE ANÚNCIOS DE RÁDIO
  Versão 5.0 — Correções de falsos positivos
  Melhorias:
    - Frases de conversa/nostalgia adicionadas ao NON_AD_PHRASES
    - Confiança "alta" exige CTA explícito OU preço/telefone
    - Detecção de locução em 1ª pessoa bloqueia classificação
    - Prompt LLM reforçado com instrução de contexto conversacional
    - Rebaixamento automático de confiança sem âncora concreta
=============================================================
"""

STATIONS = {
    "Band_FM":      "https://stm.alphanetdigital.com.br:7040/band",
    "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream",
    #"Jovem_Pan":   "https://sc1s.cdn.upx.com:9986/stream?1772563648730",
}

RECORD_DURATION    = 30
GROQ_WHISPER_MODEL = "whisper-large-v3-turbo"
GROQ_LLM_MODEL     = "llama-3.1-8b-instant"
MIN_SPEECH_RATIO   = 0.40
MIN_SPEECH_SEGS    = 3
TRANSCRIPTION_CAP  = 500   # 30s de fala ≈ 300-350 palavras

import subprocess, datetime, os, re, time, shutil, json, unicodedata
import threading, queue, traceback
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import librosa, torch
from groq import Groq
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY não encontrada!")

groq_client = Groq(api_key=GROQ_API_KEY)

try:
    TZ_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_BR = None

def br_now():       return datetime.datetime.now(TZ_BR) if TZ_BR else datetime.datetime.now()
def br_timestamp(): return br_now().strftime("%d-%m-%Y_%H-%M-%S")
def br_display():   return br_now().strftime("%d/%m/%Y %H:%M:%S")

def safe_filename(text: str, max_len: int = 60) -> str:
    if not text: return "Desconhecido"
    text = unicodedata.normalize("NFKD", str(text).strip())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\- ]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text).strip("_")
    return (text or "Desconhecido")[:max_len]


# ─── Heurística reforçada ─────────────────────────────────────────────────────

STRONG_AD_KEYWORDS = [
    "promoção", "oferta", "desconto", "imperdível", "compre", "aproveite", "garanta",
    "só hoje", "últimos dias", "parcel", "sem juros", "frete", "cupom", "código",
    "whatsapp", "zap", "ligue", "telefone", "disque", "site", ".com", ".br",
    "instagram", "@", "delivery", "preço", "reais", "r$", "por apenas", "a partir de",
    "vem pra", "venha", "visite", "confira", "peça já", "acesse", "baixe", "clique",
]

BUSINESS_WORDS = [
    "farmácia", "drogaria", "autoescola", "mercado", "supermercado", "clínica",
    "laboratório", "ótica", "loja", "móveis", "colchões", "academia", "concessionária",
    "pizzaria", "lanchonete", "restaurante", "pet shop", "seguro", "consórcio",
    "financiamento", "imobiliária", "construtora", "hospital", "quilo", "posto",
    "oficina", "hotel", "pousada", "colégio", "faculdade", "curso", "aplicativo",
]

NON_AD_PHRASES = [
    # Jornalismo / notícias
    "segundo informações", "de acordo com", "o governador", "o prefeito",
    "a prefeitura", "a polícia", "os bombeiros", "o governo", "a câmara",
    "o congresso", "o presidente", "a secretaria", "a temperatura",
    # Locução / transição
    "boa tarde", "bom dia", "boa noite", "você está ouvindo",
    "próxima música", "agora vamos", "fique ligado",
    # ── NOVO: Conversa/opinião em 1ª pessoa e nostalgia ──
    "eu sinto falta", "eu lembro", "lembro quando", "lembro que",
    "antigamente", "de antigamente", "era melhor", "era diferente",
    "quando eu era", "quando a gente", "na minha época",
    "cara,", "né, cara", "sabe como é", "você lembra",
    "que saudade", "minha avó", "meu pai", "minha mãe",
    "a gente fazia", "a gente comia", "a gente ia",
    "hoje em dia", "mudou muito", "não é mais assim",
]

# CTAs explícitos que ancoram confiança "alta"
EXPLICIT_CTA = [
    "ligue", "acesse", "compre", "whatsapp", "zap", "visite",
    "peça já", "clique", "baixe", "chame no", "manda mensagem",
    "fale com", "entre em contato", "vá ao site", "pelo site",
    "no instagram", "no face", "no aplicativo",
]

# Padrões de locução em 1ª pessoa (conversa de locutor/entrevistado)
FIRST_PERSON_PATTERNS = re.compile(
    r"\b(eu |a gente |nós |minha |meu |nossa |nosso )"
    r"(sinto|lembro|acho|gosto|quero|fazia|comia|ia|era|fui|vim|tenho|tinha)\b",
    re.IGNORECASE,
)


def heuristic_score(text: str) -> dict:
    if not text:
        return {
            "ad_score": 0, "nonad_score": 0,
            "has_price": False, "has_phone": False,
            "has_cta": False, "is_first_person_chat": False,
        }
    t = text.lower()

    price  = bool(re.search(r"r\$\s*\d+([.,]\d{2})?|\d+\s*reais", t))
    phone  = bool(re.search(r"(\(?\d{2}\)?\s*)?\d{4,5}[-\s]?\d{4}", t))
    has_cta = any(k in t for k in EXPLICIT_CTA)

    strong = sum(1 for k in STRONG_AD_KEYWORDS if k in t)
    biz    = sum(1 for k in BUSINESS_WORDS    if k in t)
    nonad  = sum(1 for k in NON_AD_PHRASES    if k in t)

    # Detecta conversa casual em 1ª pessoa
    is_first_person_chat = bool(FIRST_PERSON_PATTERNS.search(text))
    if is_first_person_chat:
        nonad += 3  # peso extra para bloquear LLM

    ad_score = strong * 2 + biz + (3 if price else 0) + (2 if phone else 0) + (2 if has_cta else 0)

    return {
        "ad_score": ad_score,
        "nonad_score": nonad,
        "has_price": price,
        "has_phone": phone,
        "has_cta": has_cta,
        "is_first_person_chat": is_first_person_chat,
    }


# ─── Worker de gravação ───────────────────────────────────────────────────────

def recorder_worker(name, url, audio_path, work_queue, stop_event):
    print(f"  🎙️  Gravador iniciado: {name}")
    while not stop_event.is_set():
        ts        = br_timestamp()
        file_path = os.path.join(audio_path, f"{safe_filename(name)}_{ts}.mp3")
        cmd = [
            "ffmpeg", "-i", url, "-t", str(RECORD_DURATION),
            "-acodec", "libmp3lame", "-ar", "16000", "-ac", "1",
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


# ─── Classe principal ─────────────────────────────────────────────────────────

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
            repo_or_dir="snakers4/silero-vad", model="silero_vad", trust_repo=True,
        )
        self.get_speech_timestamps = utils[0]
        print(f"☁️  Groq | Whisper: {GROQ_WHISPER_MODEL} | LLM: {GROQ_LLM_MODEL} | Ciclo: {RECORD_DURATION}s")
        self._init_excel()
        print("✅ Pronto.\n")

    # ── Excel ─────────────────────────────────────────────────────────────────

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
        hfill  = PatternFill("solid", start_color="1F4E79")
        hfont  = Font(bold=True, color="FFFFFF", name="Arial", size=11)
        border = Border(**{s: Side(style="thin", color="CCCCCC") for s in ("left", "right", "bottom")})
        for col, h in enumerate(headers, 1):
            c = ws.cell(row=1, column=col, value=h)
            c.font = hfont; c.fill = hfill
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = border
        ws.row_dimensions[1].height = 30
        ws.freeze_panes = "A2"
        for i, w in enumerate([20, 15, 22, 22, 12, 60, 40], 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws2 = wb.create_sheet("Resumo por Rádio")
        for col, h in enumerate(["Rádio", "Total de Anúncios", "Última Detecção"], 1):
            c = ws2.cell(row=1, column=col, value=h)
            c.font = Font(bold=True, color="FFFFFF", name="Arial")
            c.fill = PatternFill("solid", start_color="2E75B6")
        wb.save(self.report_path)
        print(f"📊 Relatório criado: {self.report_path}")

    def _append_to_excel(self, station, info, snippet, audio_file):
        try:
            wb  = load_workbook(self.report_path)
            ws  = wb["Anúncios Detectados"]
            row = ws.max_row + 1
            conf = info.get("confianca", "baixa")
            fill = PatternFill("solid", start_color={
                "alta": "E2EFDA", "media": "FFF2CC", "baixa": "FCE4D6",
            }.get(conf, "FFFFFF"))
            font = Font(name="Arial", size=10)
            bord = Border(**{s: Side(style="thin", color="DDDDDD") for s in ("left", "right", "bottom")})
            for col, val in enumerate([
                br_display(), station,
                info.get("anunciante") or "—", info.get("produto") or "—",
                conf.upper(), (snippet or "")[:200], os.path.basename(audio_file),
            ], 1):
                c = ws.cell(row=row, column=col, value=val)
                c.font = font; c.fill = fill; c.border = bord
                c.alignment = Alignment(vertical="center", wrap_text=(col == 6))
            ws.row_dimensions[row].height = 18
            ws2 = wb["Resumo por Rádio"]
            sr  = next((r for r in ws2.iter_rows(min_row=2) if r[0].value == station), None)
            if sr:
                sr[1].value = (sr[1].value or 0) + 1
                sr[2].value = br_display()
            else:
                nr = ws2.max_row + 1
                ws2.cell(row=nr, column=1, value=station)
                ws2.cell(row=nr, column=2, value=1)
                ws2.cell(row=nr, column=3, value=br_display())
            wb.save(self.report_path)
            print(f"  ✅ Excel salvo ({row - 1} anúncios)")
        except Exception as e:
            print(f"  ⚠️  Erro ao salvar Excel: {e}"); traceback.print_exc()

    # ── VAD ───────────────────────────────────────────────────────────────────

    def analyze_vad(self, file_path):
        try:
            y, sr = librosa.load(file_path, sr=16000)
            segs  = self.get_speech_timestamps(
                torch.from_numpy(y).float(), self.vad_model, sampling_rate=16000,
            )
            if not segs: return None
            duration     = len(y) / sr
            total_speech = sum((s["end"] - s["start"]) / sr for s in segs)
            ratio        = total_speech / duration
            if ratio < MIN_SPEECH_RATIO or len(segs) < MIN_SPEECH_SEGS:
                return None
            return {"speech_ratio": ratio, "fragments": len(segs)}
        except Exception as e:
            print(f"  ⚠️  Erro VAD: {e}"); return None

    # ── Transcrição ───────────────────────────────────────────────────────────

    def transcribe(self, file_path):
        try:
            with open(file_path, "rb") as f:
                r = groq_client.audio.transcriptions.create(
                    file=(os.path.basename(file_path), f),
                    model=GROQ_WHISPER_MODEL, language="pt", response_format="text",
                )
            return (r if isinstance(r, str) else str(r)).strip()
        except Exception as e:
            print(f"  ⚠️  Erro Whisper: {e}"); traceback.print_exc(); return ""

    # ── Validação de confiança ────────────────────────────────────────────────

    def _validate_confidence(self, conf: str, heur: dict) -> str:
        """
        Rebaixa confiança "alta" para "media" quando não há âncora concreta:
        preço, telefone ou CTA explícito.
        Sem nenhuma âncora, "media" vira "baixa".
        """
        has_anchor = heur["has_price"] or heur["has_phone"] or heur["has_cta"]

        if conf == "alta" and not has_anchor:
            print("  ⬇️  Confiança rebaixada: alta → media (sem preço/telefone/CTA)")
            return "media"

        if conf == "media" and not has_anchor and heur["ad_score"] < 4:
            print("  ⬇️  Confiança rebaixada: media → baixa (sem âncora + score baixo)")
            return "baixa"

        return conf

    # ── Classificação compacta ────────────────────────────────────────────────

    def classify_multi(self, transcription: str) -> list:
        """
        Retorna lista de anúncios aprovados.
        Chama o LLM apenas quando necessário.
        """
        text = (transcription or "").strip()
        if len(text) < 25:
            return []

        heur = heuristic_score(text)

        # ── Bloqueio 1: claramente não é anúncio
        if heur["ad_score"] < 2 and heur["nonad_score"] >= 2:
            return []

        # ── Bloqueio 2: conversa em 1ª pessoa sem qualquer âncora de anúncio
        if heur["is_first_person_chat"] and not heur["has_price"] and not heur["has_cta"]:
            print("  🗣️  Ignorado: locução em 1ª pessoa sem âncora de anúncio.")
            return []

        # ── Shortcut: score muito alto + preço → registra sem chamar LLM
        if heur["ad_score"] >= 8 and heur["has_price"]:
            return [{
                "eh_anuncio": True, "anunciante": None, "produto": None,
                "confianca": "media",
                "motivo_curto": "Score heurístico alto (sem LLM)",
                "trecho": "",
            }]

        # ── Prompt reforçado com instrução de contexto conversacional
        dica = (
            "ATENÇÃO: alta probabilidade de anúncio." if heur["ad_score"] >= 4
            else "Pode haver anúncio." if heur["ad_score"] >= 2
            else ""
        )
        prompt = (
            f"Classifique os anúncios publicitários nesta transcrição de rádio brasileiro ({RECORD_DURATION}s).\n"
            f"Pode haver zero, um ou mais anúncios distintos.\n\n"
            f"REGRAS OBRIGATÓRIAS:\n"
            f"- Anúncio = marca/empresa real + venda/promoção/CTA (ex: ligue, acesse, compre, whatsapp).\n"
            f"- Ignore: notícias, locução, letras de música, entrevistas, conversa casual.\n"
            f"- Conversa em 1ª pessoa ('eu sinto', 'eu lembro', 'a gente fazia', 'antigamente') "
            f"indica opinião/nostalgia do locutor — NÃO é anúncio, mesmo que mencione produtos.\n"
            f"- Sem CTA explícito E sem preço/telefone → confiança máxima = 'media'.\n"
            f"- Só use 'alta' se houver CTA explícito (ligue, acesse, compre etc.) OU preço/telefone.\n"
            f"{dica}\n\n"
            f"Texto:\n\"\"\"{text}\"\"\"\n\n"
            f"Responda APENAS JSON:\n"
            f'{{"anuncios":[{{"anunciante":"...","produto":"...","confianca":"alta|media|baixa","trecho":"...max80chars"}}]}}\n'
            f"Se não houver: {{\"anuncios\":[]}}"
        )

        try:
            resp = groq_client.chat.completions.create(
                model=GROQ_LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0, max_tokens=600,
                response_format={"type": "json_object"},
            )
            raw  = resp.choices[0].message.content or ""
            data = {}
            try:
                data = json.loads(raw)
            except Exception:
                m = re.search(r"\{.*\}", raw, flags=re.S)
                data = json.loads(m.group(0)) if m else {}

            anuncios = data.get("anuncios", [])
            if not isinstance(anuncios, list):
                return []

            aprovados = []
            seen      = set()

            for ad in anuncios:
                if not isinstance(ad, dict): continue

                anunciante = (ad.get("anunciante") or "").strip() or None
                if anunciante and anunciante.lower() in ("null", "none"):
                    anunciante = None

                produto = (ad.get("produto") or "").strip() or None
                if produto and produto.lower() in ("null", "none"):
                    produto = None

                conf = str(ad.get("confianca", "baixa")).lower().strip()
                if conf not in ("alta", "media", "baixa"):
                    conf = "baixa"

                # ── Valida e potencialmente rebaixa confiança
                conf = self._validate_confidence(conf, heur)

                # Deduplicar por anunciante
                chave = (anunciante or "").lower()
                if chave and chave in seen: continue
                if chave: seen.add(chave)

                item = {
                    "eh_anuncio": True,
                    "anunciante": anunciante,
                    "produto": produto,
                    "confianca": conf,
                    "motivo_curto": ad.get("motivo_curto", ""),
                    "trecho": ad.get("trecho", ""),
                }

                # Aprovação por confiança
                if conf == "alta":
                    aprovados.append(item)
                elif conf == "media" and heur["ad_score"] >= 2:
                    aprovados.append(item)
                elif conf == "baixa" and heur["ad_score"] >= 5 and anunciante:
                    item["confianca"] = "media"
                    aprovados.append(item)

            # Fallback heurístico final
            if not aprovados and heur["ad_score"] >= 6 and heur["has_price"]:
                aprovados.append({
                    "eh_anuncio": True, "anunciante": None, "produto": None,
                    "confianca": "baixa",
                    "motivo_curto": "Detectado por heurística (preço + keywords)",
                    "trecho": "",
                })

            return aprovados

        except Exception as e:
            print(f"  ⚠️  Erro LLM: {e}"); return []

    # ── Salvar áudio ──────────────────────────────────────────────────────────

    def save_ad(self, station, audio_file, info, index=0):
        marca   = safe_filename(info.get("anunciante") or "Desconhecido")
        produto = safe_filename(info.get("produto")    or "")
        parts   = [safe_filename(station), marca]
        if produto and produto != "Desconhecido": parts.append(produto)
        parts.append(br_timestamp())
        if index > 0: parts.append(f"ad{index}")
        dest = os.path.join(self.ads_path, "__".join(parts) + ".mp3")
        shutil.copy2(audio_file, dest)
        return dest

    # ── Processar item ────────────────────────────────────────────────────────

    def process_item(self, name, audio_file):
        try:
            vad = self.analyze_vad(audio_file)
            if not vad:
                print(f"  🎵 [{name}] Ignorado (pouca fala)"); return

            print(
                f"  🔍 [{name}] Transcrevendo... "
                f"(speech={vad['speech_ratio']:.0%}, frags={vad['fragments']})"
            )
            text    = self.transcribe(audio_file)
            snippet = text[:TRANSCRIPTION_CAP]

            if not snippet:
                print(f"  ⚠️  [{name}] Transcrição vazia."); return

            # Filtro heurístico pré-LLM
            heur = heuristic_score(snippet)

            if heur["ad_score"] < 2 and heur["nonad_score"] >= 2:
                print(f"  🎵 [{name}] Descartado por heurística (jornalístico/locutor).")
                return

            # Bloqueio de conversa casual sem âncora
            if heur["is_first_person_chat"] and not heur["has_price"] and not heur["has_cta"]:
                print(f"  🗣️  [{name}] Descartado: conversa em 1ª pessoa sem preço/CTA.")
                return

            print(f"  📝 [{name}] {snippet[:200].replace(chr(10), ' ')!r}")
            anuncios = self.classify_multi(snippet)

            if not anuncios:
                print(f"  🎵 [{name}] Nenhum anúncio detectado."); return

            print(f"  📢 [{name}] {len(anuncios)} anúncio(s)!")
            for i, info in enumerate(anuncios, 1):
                marca = info.get("anunciante") or "Desconhecido"
                conf  = info.get("confianca", "media")
                print(f"       [{i}] {marca} (conf={conf}) — {info.get('trecho', '')[:80]}")
                idx   = i if len(anuncios) > 1 else 0
                saved = self.save_ad(name, audio_file, info, index=idx)
                self._append_to_excel(name, info, snippet, saved)
                print(f"       💾 {os.path.basename(saved)}")

        except Exception as e:
            print(f"  ❌ [{name}] Erro: {e}"); traceback.print_exc()
        finally:
            if os.path.exists(audio_file): os.remove(audio_file)

    # ── Loop principal ────────────────────────────────────────────────────────

    def run(self):
        print("🚀 Monitoramento iniciado...")
        print(f"   Rádios   : {', '.join(STATIONS.keys())}")
        print(f"   Duração  : {RECORD_DURATION}s | Relatório: {os.path.abspath(self.report_path)}")
        print("   (Ctrl+C para parar)\n")

        work_queue = queue.Queue()
        stop_event = threading.Event()
        threads    = []

        for name, url in STATIONS.items():
            t = threading.Thread(
                target=recorder_worker,
                args=(name, url, self.audio_path, work_queue, stop_event),
                daemon=True, name=f"rec-{name}",
            )
            t.start(); threads.append(t)

        print(f"  🎙️  {len(threads)} gravadores iniciados.\n")
        try:
            while True:
                try:
                    name, audio_file = work_queue.get(timeout=2)
                    print(f"\n{'─'*60}\n📥 [{name}] Novo áudio — {br_display()}")
                    self.process_item(name, audio_file)
                    work_queue.task_done()
                except queue.Empty:
                    continue
        except KeyboardInterrupt:
            print("\n\n🛑 Encerrando...")
            stop_event.set()
            for t in threads: t.join(timeout=5)
            print("👋 Encerrado.")


if __name__ == "__main__":
    AdDetector().run()