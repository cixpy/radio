import subprocess
import datetime
import os
import re
import numpy as np
import librosa
import torch
import json
import time
import shutil
import requests
import whisper
from zoneinfo import ZoneInfo
import unicodedata

# Configurações Ollama
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3:4b"


# ========= TIMEZONE BR (Windows-safe) =========
try:
    TZ_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_BR = None


def br_timestamp():
    # Ex: 04-03-2026_10-21-33
    now = datetime.datetime.now(TZ_BR) if TZ_BR else datetime.datetime.now()
    return now.strftime("%d-%m-%Y_%H-%M-%S")


# ========= UTIL =========
def safe_filename(text: str, max_len: int = 60) -> str:
    if not text:
        return "Desconhecido"

    text = str(text).strip()

    # Remove acentos
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))

    # Caracteres seguros
    text = re.sub(r"[^\w\- ]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text).strip("_")

    return (text or "Desconhecido")[:max_len]


def looks_like_ad_text(t: str) -> bool:
    """
    Heurística rápida: ajuda a aceitar confiança "média" quando tem cara de anúncio.
    Mas NÃO bloqueia o Ollama (só influencia a decisão final).
    """
    if not t:
        return False

    t_low = t.lower()

    strong = [
        "promo", "promoção", "oferta", "desconto", "imperdível",
        "compre", "aproveite", "garanta", "só hoje", "últimos dias",
        "parcel", "sem juros", "frete", "cupom", "código", "dinheiro"
        "whatsapp", "zap", "ligue", "telefone", "disque", "catanduva",
        "site", ".com", ".br", "instagram", "@", "delivery",
        "preço", "reais", "r$", "por apenas", "a partir de",
        # rádio-style
        "vem pra", "venha", "visite", "confira", "peça já",
    ]

    business_words = [
        "farmácia", "drogaria", "autoescola", "mercado", "supermercado",
        "clínica", "laboratório", "ótica", "loja", "móveis", "colchões",
        "academia", "concessionária", "pizzaria", "lanchonete", "restaurante",
        "pet shop", "seguro", "consórcio", "financiamento", "imobiliária", "construtora","hospital",
        "academia", "concessionária", "pizzaria", "lanchonete", "restaurante",
    ]

    price_pattern = re.search(r"(r\$)\s*\d+([.,]\d{2})?", t_low)
    phone_pattern = re.search(r"(\(?\d{2}\)?\s*)?\d{4,5}[-\s]?\d{4}", t_low)

    hits = sum(1 for k in strong if k in t_low)
    biz_hits = sum(1 for k in business_words if k in t_low)

    return hits >= 2 or bool(price_pattern) or bool(phone_pattern) or (biz_hits >= 1 and hits >= 1)


class AdDetector:
    def __init__(self, whisper_model_size="small"):
        self.stations = {
            "Band_FM": "https://stm.alphanetdigital.com.br:7040/band",
            "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream"
        }

        self.base_path = "radio_capture"
        self.audio_path = os.path.join(self.base_path, "temp_audios")
        self.log_path = os.path.join(self.base_path, "logs")
        self.ads_path = os.path.join(self.base_path, "detected_ads")

        for folder in [self.audio_path, self.log_path, self.ads_path]:
            os.makedirs(folder, exist_ok=True)

        print("🔧 Carregando Silero VAD...")
        self.model, utils = torch.hub.load(
            repo_or_dir="snakers4/silero-vad",
            model="silero_vad",
            trust_repo=True
        )
        self.get_speech_timestamps = utils[0]

        print(f"🔧 Carregando Whisper ({whisper_model_size})...")
        self.whisper_model = whisper.load_model(whisper_model_size)

        print("✅ Pronto.\n")

    def record_radio(self, name, url, duration=30):
        ts = br_timestamp()
        file_path = os.path.join(self.audio_path, f"{safe_filename(name)}_{ts}.mp3")

        command = [
            "ffmpeg", "-i", url, "-t", str(duration),
            "-acodec", "libmp3lame", "-ar", "16000", "-ac", "1",
            file_path, "-y", "-loglevel", "quiet"
        ]
        try:
            subprocess.run(command, check=True, timeout=duration + 15)
            return file_path
        except Exception as e:
            print(f"Erro ao gravar {name}: {e}")
            return None

    def analyze_audio_vad(self, file_path):
        """
        VAD: garante que tem fala suficiente pra valer Whisper/LLM.
        """
        try:
            y, sr = librosa.load(file_path, sr=16000)
            wav_tensor = torch.from_numpy(y).float()

            speech_segments = self.get_speech_timestamps(
                wav_tensor, self.model, sampling_rate=16000
            )

            if not speech_segments:
                return None

            duration = len(y) / sr
            total_speech = sum([(s["end"] - s["start"]) / sr for s in speech_segments])
            speech_ratio = total_speech / duration

            if speech_ratio < 0.30:
                return None

            if len(speech_segments) < 2:
                return None

            return {"speech_ratio": speech_ratio, "fragments": len(speech_segments)}
        except Exception as e:
            print(f"Erro na análise VAD: {e}")
            return None

    def _ollama_classify(self, transcription: str) -> dict:
        prompt = f"""
Você é um classificador de rádio brasileiro.
Decida se o texto é um ANÚNCIO PUBLICITÁRIO (propaganda) ou CONTEÚDO normal (música/locutor/jornal).
Regras:
- Se houver oferta, call-to-action, nome de empresa vendendo algo, endereço, telefone, preço, parcelamento etc -> provável anúncio.
- Se for só locução informativa (notícia, boletim, conversa) sem venda -> não é anúncio.

Texto:
\"\"\"{transcription}\"\"\"

Responda SOMENTE JSON válido:
{{
  "eh_anuncio": true/false,
  "anunciante": "marca" ou null,
  "produto": "produto/serviço" ou null,
  "confianca": "alta" | "media" | "baixa",
  "motivo_curto": "1 frase"
}}
""".strip()

        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False
                },
                timeout=120
            )
            res_json = response.json()
            raw = res_json.get("response", "")

            # Parse tolerante
            if isinstance(raw, dict):
                data = raw
            else:
                raw = (raw or "").strip()
                try:
                    data = json.loads(raw)
                except Exception:
                    m = re.search(r"\{.*\}", raw, flags=re.S)
                    data = json.loads(m.group(0)) if m else {}

            # Normalização
            conf = str(data.get("confianca", "baixa")).lower().strip()
            if conf not in ("alta", "media", "baixa"):
                conf = "baixa"
            data["confianca"] = conf

            for k in ("anunciante", "produto"):
                if isinstance(data.get(k), str):
                    v = data[k].strip()
                    if v.lower() in ("", "null", "none"):
                        data[k] = None

            data["eh_anuncio"] = bool(data.get("eh_anuncio", False))
            return data

        except Exception as e:
            print(f"Erro ao consultar Ollama: {e}")
            return {"eh_anuncio": False, "confianca": "baixa", "anunciante": None, "produto": None}

    def identify_content(self, transcription: str) -> dict:
        """
        Agora é o conserto REAL:
        - NÃO bloqueia o Ollama quando não tem palavra de anúncio.
        - Usa heurística só pra decidir o quanto aceitar "média".
        """
        transcription = (transcription or "").strip()

        if len(transcription) < 25:
            return {"eh_anuncio": False, "confianca": "baixa", "anunciante": None, "produto": None}

        heuristic = looks_like_ad_text(transcription)
        data = self._ollama_classify(transcription)

        eh = bool(data.get("eh_anuncio", False))
        conf = data.get("confianca", "baixa")
        anunciante = data.get("anunciante")

        if heuristic:
            # Se já parece anúncio, aceita média/alta
            if eh and conf in ("alta", "media"):
                return data
        else:
            # Se NÃO parece anúncio, só aceita se for muito forte:
            # - alta confiança OU conseguiu extrair anunciante
            if eh and (conf == "alta" or anunciante):
                return data

        return {"eh_anuncio": False, "confianca": conf, "anunciante": None, "produto": None}

    def save_ad(self, station_name: str, audio_file: str, info: dict):
        marca = safe_filename(info.get("anunciante") or "Desconhecido")
        produto = safe_filename(info.get("produto") or "")
        ts = br_timestamp()

        parts = [safe_filename(station_name), marca]
        if produto and produto != "Desconhecido":
            parts.append(produto)
        parts.append(ts)

        filename = "__".join(parts) + ".mp3"
        dest = os.path.join(self.ads_path, filename)

        shutil.copy2(audio_file, dest)
        return dest

    def process_loop(self):
        print("🚀 Monitorando rádio... (Ctrl+C para parar)\n")

        while True:
            for name, url in self.stations.items():
                print(f"Ouvindo {name}...")
                audio_file = self.record_radio(name, url, duration=30)

                if not audio_file or not os.path.exists(audio_file):
                    continue

                try:
                    vad_result = self.analyze_audio_vad(audio_file)
                    if not vad_result:
                        print(f"🎵 [IGNORADO] {name} (pouca fala)")
                        continue

                    print(f"Processando áudio de {name}... (speech_ratio={vad_result['speech_ratio']:.2f})")

                    result = self.whisper_model.transcribe(audio_file, language="pt")
                    text = (result.get("text") or "").strip()
                    snippet = text[:1200]

                    print("📝 TRANSCRIÇÃO (300 chars):", snippet[:300].replace("\n", " "))

                    info = self.identify_content(snippet)

                    if info.get("eh_anuncio"):
                        marca = info.get("anunciante") or "Desconhecido"
                        conf = info.get("confianca", "media")
                        print(f"📢 [ANÚNCIO] {name} -> {marca} (conf={conf})")

                        saved = self.save_ad(name, audio_file, info)
                        print(f"💾 Salvo em: {saved}")
                    else:
                        print(f"🎵 [IGNORADO] {name} (não é anúncio)")

                finally:
                    if os.path.exists(audio_file):
                        os.remove(audio_file)

            time.sleep(5)


if __name__ == "__main__":
    detector = AdDetector(whisper_model_size="small")
    detector.process_loop()