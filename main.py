import subprocess
import datetime
import os
import numpy as np
import librosa
import torch
import json
import time
import shutil
import requests
import whisper
from pathlib import Path

# Configurações Ollama
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3:4b"

class AdDetector:
    def __init__(self, whisper_model_size="small"):
        self.stations = {
            "Band_FM": "https://stm.alphanetdigital.com.br:7040/band",
            "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream"
        }

        self.base_path = "radio_capture"
        self.audio_path = f"{self.base_path}/temp_audios"
        self.log_path = f"{self.base_path}/logs"
        self.ads_path = f"{self.base_path}/detected_ads"

        for folder in [self.audio_path, self.log_path, self.ads_path]:
            Path(folder).mkdir(parents=True, exist_ok=True)

        print("🔧 Carregando Silero VAD...")
        self.model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad', model='silero_vad', trust_repo=True)
        self.get_speech_timestamps = utils[0]

        print(f"🔧 Carregando Whisper ({whisper_model_size})...")
        self.whisper_model = whisper.load_model(whisper_model_size)
        print("✅ Pronto.\n")

    def record_radio(self, name, url, duration=30):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{self.audio_path}/{name}_{timestamp}.mp3"
        command = ['ffmpeg', '-i', url, '-t', str(duration), '-acodec', 'libmp3lame', '-ar', '16000', '-ac', '1', file_path, '-y', '-loglevel', 'quiet']
        try:
            subprocess.run(command, check=True, timeout=duration + 10)
            return file_path
        except:
            return None

    def analyze_audio_vad(self, file_path):
        """Filtro inicial: Anúncios costumam ser rápidos e picotados."""
        try:
            y, sr = librosa.load(file_path, sr=16000)
            wav_tensor = torch.from_numpy(y).float()
            speech_segments = self.get_speech_timestamps(wav_tensor, self.model, sampling_rate=16000)
            
            duration = len(y) / sr
            total_speech = sum([(s['end'] - s['start']) / 16000 for s in speech_segments])
            speech_ratio = total_speech / duration

            # Se for silêncio demais ou música instrumental (pouca voz), ignora
            if speech_ratio < 0.25: return None
            
            return {"speech_ratio": speech_ratio, "fragments": len(speech_segments)}
        except:
            return None

    def identify_content(self, transcription):
        """Usa o LLM para decidir se é anúncio ou música."""
        if len(transcription) < 20: return {"eh_anuncio": False}

        prompt = f"""Analise este texto de rádio e determine se é um ANÚNCIO PUBLICITÁRIO ou MÚSICA/CONVERSA.
TEXTO: "{transcription}"

Responda APENAS em JSON:
{{
  "eh_anuncio": true/false,
  "anunciante": "nome da marca ou null",
  "produto": "item vendido ou null",
  "confianca": "alta/media/baixa"
}}"""

        try:
            res = requests.post(OLLAMA_URL, json={"model": OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False}, timeout=30)
            return json.loads(res.json().get("response", "{}"))
        except:
            return {"eh_anuncio": False}

    def process_loop(self):
        print("🚀 Monitorando rádio... (Pressione Ctrl+C para parar)\n")
        while True:
            for name, url in self.stations.items():
                audio_file = self.record_radio(name, url)
                if not audio_file: continue

                vad = self.analyze_audio_vad(audio_file)
                if vad:
                    text = self.whisper_model.transcribe(audio_file, language="pt", fp16=False)["text"]
                    
                    # Validação com o Gemma
                    info = self.identify_content(text)
                    
                    if info.get("eh_anuncio"):
                        marca = info.get("anunciante") or "Desconhecido"
                        print(f"📢 [ANÚNCIO] {name} -> {marca}")
                        
                        dest = f"{self.ads_path}/{name}_{marca}_{int(time.time())}.mp3"
                        shutil.copy2(audio_file, dest)
                    else:
                        print(f"🎵 [IGNORADO] {name} (Música ou locução)")

                if os.path.exists(audio_file): os.remove(audio_file)
            time.sleep(5)

if __name__ == "__main__":
    detector = AdDetector()
    detector.process_loop()