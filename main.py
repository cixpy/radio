import subprocess
import datetime
import os
import numpy as np
import librosa
import torch
import json
import time
import shutil
from pathlib import Path

class AdDetector:
    def __init__(self):
        # Radio stations URLs
        self.stations = {
            "Jovem_Pan": "https://sc1s.cdn.upx.com:9986/stream",
            "Band_FM": "https://stm.alphanetdigital.com.br:7040/band",
            "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream"
        }
        
        # Paths
        self.base_path = "radio_capture"
        self.audio_path = f"{self.base_path}/audios"
        self.log_path = f"{self.base_path}/logs"
        self.ads_path = f"{self.base_path}/detected_ads"
        
        for folder in [self.audio_path, self.log_path, self.ads_path]:
            Path(folder).mkdir(parents=True, exist_ok=True)

        # Load Silero VAD Model (AI)
        # We use trust_repo=True to allow torch to download the model
        self.model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad', 
                                          model='silero_vad', 
                                          trust_repo=True)
        self.get_speech_timestamps = utils[0]

    def record_radio(self, name, url, duration=30):
        """Records stream using FFmpeg"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{self.audio_path}/{name}_{timestamp}.mp3"
        
        command = [
            'ffmpeg', '-i', url, '-t', str(duration), 
            '-acodec', 'libmp3lame', '-ar', '16000', '-ac', '1',
            file_path, '-y', '-loglevel', 'quiet'
        ]
        
        try:
            subprocess.run(command, check=True, timeout=duration + 15)
            return file_path
        except Exception as e:
            print(f"Error recording {name}: {e}")
            return None

    def analyze_audio(self, file_path):
        """AI analysis without using torchaudio/torchcodec"""
        try:
            # 1. Load audio with librosa (Very stable)
            y, sr = librosa.load(file_path, sr=16000)
            
            # 2. Convert to torch tensor for the AI model
            wav_tensor = torch.from_numpy(y).float()
            
            # 3. Run AI detection (Speech/Voice)
            with torch.no_grad():
                speech_segments = self.get_speech_timestamps(wav_tensor, self.model, sampling_rate=16000)
            
            # 4. Logic to identify Ads
            duration = len(y) / sr
            total_speech = sum([(s['end'] - s['start']) / 16000 for s in speech_segments])
            speech_ratio = total_speech / duration if duration > 0 else 0
            
            is_ad = False
            reasons = []

            # Criteria 1: Too much talking (usually Ads or Talk Shows)
            if speech_ratio > 0.45:
                is_ad = True
                reasons.append(f"High speech density: {speech_ratio:.1%}")
            
            # Criteria 2: Many rapid cuts in speech (typical for radio commercials)
            if len(speech_segments) > 6:
                is_ad = True
                reasons.append(f"Fragments detected: {len(speech_segments)}")

            return {
                "is_ad": is_ad,
                "speech_ratio": float(speech_ratio),
                "fragments": len(speech_segments),
                "reasons": reasons
            }

        except Exception as e:
            print(f"Analysis failed: {e}")
            return None

    def process_loop(self, interval=10):
        """Main monitoring loop"""
        print(f"🚀 Monitoring started on Python {datetime.datetime.now().year}...")
        try:
            while True:
                for name, url in self.stations.items():
                    print(f"--- Checking {name} ---")
                    audio_file = self.record_radio(name, url)
                    
                    if audio_file and os.path.exists(audio_file):
                        result = self.analyze_audio(audio_file)
                        
                        if result:
                            label = "📢 AD DETECTED" if result["is_ad"] else "🎵 MUSIC/CONTENT"
                            print(f"Result: {label} | Speech: {result['speech_ratio']:.1%}")
                            
                            if result["is_ad"]:
                                dest = f"{self.ads_path}/AD_{os.path.basename(audio_file)}"
                                shutil.copy2(audio_file, dest)
                            
                            self.log_to_file(name, result)
                        
                        # Cleanup: remove raw audio to save space
                        # os.remove(audio_file) 
                
                print(f"\nCycle finished. Sleeping {interval}s...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped.")

    def log_to_file(self, station, result):
        log_file = f"{self.log_path}/log_{datetime.datetime.now().strftime('%Y%m%d')}.json"
        entry = {"timestamp": datetime.datetime.now().isoformat(), "station": station, "data": result}
        
        history = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                try: history = json.load(f)
                except: pass
        
        history.append(entry)
        with open(log_file, 'w') as f:
            json.dump(history, f, indent=2)

if __name__ == "__main__":
    detector = AdDetector()
    detector.process_loop(interval=5)