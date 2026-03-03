
import subprocess
import datetime

# Dicionário com suas rádios
radios = {
    "Jovem_Pan": "https://sc1s.cdn.upx.com:9986/stream",
    "Band_FM": "https://stm.alphanetdigital.com.br:7040/band",
    "Ondas_Verdes": "https://live3.livemus.com.br:6922/stream"
}

def gravar_radio(nome, url, segundos=60):
    timestamp = datetime.datetime.now().strftime("%d-%m-%Y_%Hh-%Mm-%Ss")
    arquivo = f"{nome}_{timestamp}.mp3"
    
    print(f"Gravando {nome}...")
    
    # Comando FFmpeg para capturar o áudio
    comando = [
        'ffmpeg', '-i', url, '-t', str(segundos), 
        '-acodec', 'libmp3lame', arquivo, '-y'
    ]
    
    subprocess.run(comando, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    print(f"Salvo: {arquivo}")

# Teste: Grava 30 segundos da Jovem Pan
gravar_radio("Band_FM", radios["Band_FM"], 30)