#/usr/bin/env python3
from ast import arg
import openai
import sys
import os

from sympy import im
from logger import get_logger
import helpers as utils
import time

LOGGER = get_logger("tts_utils", debug=True, quiet=False)


def _pygame_play(audio_file:str, logger=LOGGER):
    try:
        import pygame
        pygame.init()
        pygame.mixer.init()
        pygame.mixer.music.load(audio_file)   
        logger.debug(f"Finished playing audio file {audio_file}")
        logger.debug(f"Playing audio file {audio_file}")
        while pygame.mixer.music.get_busy():
                pygame.time.Clock().tick(10)
        pygame.mixer.music.unload()
        logger.debug(f"Finished playing audio file {audio_file}")
        return True  
    except ImportError:
        logger.error("pygame not installed")
        return False 
    except Exception as e:
        logger.error(f"playback: {e}")
        return False

def _vlc_play(audio_file:str, logger=LOGGER):
    try:
        import vlc
        media_player = vlc.MediaPlayer() 
# media object 
        media = vlc.Media(audio_file) 
# setting media to the media player 
        media_player.set_media(media) 
        media_player.play()
        time.sleep(1.5)
        while media_player.is_playing():
            time.sleep(1)
        return True
    except ImportError:
        logger.error("vlc not installed")
        return False
    except Exception as e:
        logger.error(f"playback: {e}")
        return False


def play(audio_file:str, player:str="vlc", logger=LOGGER):
 
    if not os.path.exists(audio_file):
        logger.error(f"File {audio_file} does not exist")
        return False
    
    if player == "vlc":
        return _vlc_play(audio_file, logger=logger)

    if player == "pygame":
        return _pygame_play(audio_file, logger=logger)
    
    logger.error(f"Unknown player {player}")
    return False


def play_dir(directory:str, player:str="vlc", logger=LOGGER):
    if not os.path.exists(directory):
        logger.error(f"Directory {directory} does not exist")
        return False

    files = utils.get_file_names(directory, ext="mp3", sort_by="index", logger=logger)
   
    for i, file in  enumerate(files):
        txt_file = file.replace(".mp3", ".txt")
        txt_file = os.path.join(directory,txt_file)
        if os.path.exists(txt_file):
            with open(txt_file, 'r') as f:
                text = f.read()
            logger.info(f"Playing {i} of {len(files)}: {text}")
        else:
            logger.info(f"Playing {i} of {len(files)}: {file}")
        play(os.path.join(directory,file), player=player, logger=logger)
        logger.info(f"Finished playing {i} of {len(files)}")
    return True


def combine_audio_files(directory:str, output_file:str, logger=LOGGER):
    import ffmpeg
    mp3_files = utils.get_file_names(directory, ext="mp3", sort_by="index", logger=logger)
    if not mp3_files:
        logger.error(f"No mp3 files found in directory {directory}")
        return False

    input_files = [ffmpeg.input(os.path.join(directory,file)) for file in mp3_files]
    joined = ffmpeg.concat(*input_files, v=0, a=1)
    ffmpeg.output(joined, output_file).run()

    return True



def tts_openai(text:str, audio_file:str,  voice:str="nova", logger=LOGGER)->str:
    """ call openai api to generate tts audio file and save it to disk"

    Args:
        text (str): text to convert to speech
        audio_file (str): save audio to this file
        voice (str, optional):  Defaults to "nova".
        logger (logger, optional): Defaults to LOGGER.

    Returns:
        str: audio file name or None
    """
    
    try:
        open(audio_file, 'w').close()
    except Exception as e:
        logger.error(f"create {audio_file}: {e}")
        return None

    
    if len(text) < 2:
        logger.error("Text too short. Do this check before calling tts_openai()")
        return None

    try:    
        response = openai.audio.speech.create(
            model="tts-1",
            voice=voice,
            input=f"{text}"
        )
        response.write_to_file(audio_file)
        return audio_file
    except Exception as e:
        logger.error(f"Faild to obtain tts from openai {e}")
        return None


def fetch_tts_audio(text:str, audio_file:str, provider="openai", voice="nova", logger=LOGGER):
    if provider == "openai":
        return tts_openai(text, audio_file, voice)
    else:
        logger.error(f"Unknown provider {provider}")
        return None

 
def _test():
    import argparse
    argparser =  argparse.ArgumentParser()
    argparser.add_argument("action",  type=str )
    argparser.add_argument("-t", "--text", type=str, help="TTS source text")
    argparser.add_argument("-f","--inputfile", type=str, help="TTS source file")
    argparser.add_argument("-o", "--outfile", type=str, help="Directory to save chunk and  audio files")
    argparser.add_argument("-d", "--directory", type=str, help="Directory containing audio files")  
    args = argparser.parse_args()


    if args.action == "convert":
        if(args.text):
            audio_file = tts_openai(args.text, "audio_out.mp3")
            if audio_file:
                play(audio_file)
            else:
                LOGGER.error("Failed")
            sys.exit(0)
            
        if(args.file):
            if not os.path.exists(args.file):
                LOGGER.error(f"File {args.file} does not exist")
                sys.exit(1)
            filename = args.file.split("/")[-1]
            filename = filename.split(".")[0]
            LOGGER.debug(f"Filename: {filename}")

            with open(args.file, 'r') as f:
                text = f.read()
            chunks = utils.chunk_by_words(text)
            LOGGER.debug(f"Chunked into {len(chunks)} parts")
            for i, chunk in enumerate(chunks):
                with open(f"{filename}_{i}.txt", 'w') as f:
                    f.write(chunk)
                LOGGER.debug(f"TTS Request {i} of {len(chunks)}")
                audio_file = tts_openai(chunk, f"{filename}_{i}.mp3")
                if audio_file:
                    LOGGER.error("Failed")
                    sys.exit(1)
            sys.exit(0)

    if args.action == "combine":
        if args.directory and args.outfile:
            combine_audio_files(args.directory, args.outfile )
        else:
            print("dir and outfile required", file=sys.stderr)            


     

        




if __name__ == "__main__":
    _test()
