#!/usr/bin/env python3
import chromadb
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(current_dir)
sys.path.append(parent_dir)
from logger import get_logger
from chromadb.utils import embedding_functions
import voyageai
from chromadb import Documents, EmbeddingFunction, Embeddings, Collection
from typing import List, Dict
import fitz
import json
from datetime import datetime
from data_uitls import pdf_to_txt, word_to_txt, get_file_names
from load_env import load_env
from config import CHROMADB_HOST, CHROMADB_PORT, DEBUG, QUET, DATA_DIR



class VoyageAIEmbeddingFunction(EmbeddingFunction):
    def __call__(self, input: Documents) -> Embeddings:
        vo = voyageai.Client()
        result = vo.embed(input, model="voyage-lite-02-instruct")
        return result.embeddings        
    
def chunk_by_size(text: str, chunk_size: int) -> List[str]:
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]


def get_collection(database:str,  embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    logger = get_logger(__name__, debug=DEBUG, quiet=QUET)
    try:
        if CHROMADB_HOST is "local":
            client  = chromadb.PersistentClient(path=DATA_DIR)
        else:
            client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT, ssl=True)

        
        collection = client.get_or_create_collection(name=database, embedding_function=embedding_function)
        logger.info(f"Connected to {database}")
    except Exception as e:
        logger.error(f"failed to get collection {e}")
        return None
    return collection

 
def add_files(file_list:list, database:str="default_collection", collection:Collection=None,  embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    logger = get_logger(__name__, debug=DEBUG, quiet=QUET)
    
    if collection is None:
        collection = get_collection(database, embedding_function)
    file_count = 0
    for file in file_list:
        file_count += 1 
        logger.info(f"Processing file {file_count} out {len(file_list)}\n")

        if file.endswith('.pdf'):
            doc = pdf_to_txt(file)
        elif file.endswith('.json'):
            with open(file, 'r') as f:
                doc = json.dumps(f.read())
        elif file.endswith('.docx'):
            doc = word_to_txt(file)
        elif file.endswith('.txt') or file.endswith('.xml')  :       
            with open(file, 'r') as f:
                doc = f.read()
        else:
            logger.warning(f"Ignoring Unsupported file type: {file}")
            continue
        
        logger.debug(f"Number of characters in file: {len(doc)}")
        if(len(doc)) > 1500:
            chunks = chunk_by_size(doc, 1000)
        else:
            chunks = [doc]

        logger.debug(f"Number of chunks: {len(chunks)}")
        
        embedded_chunks = embedding_function(chunks)

        metadata_per_chunk = [{
            "source": file,
            "chunk_index": i,
            } for i, _ in enumerate(chunks)]
        try:
            collection.upsert(
                ids=[f"{file}_{i}" for i in range(len(chunks))],
                documents=chunks,
                metadatas=metadata_per_chunk,
                embeddings=embedded_chunks
            )
        
        except Exception as e:
            logger.error(e)
            raise Exception(f"chroma Add failed {e}"
        )
    
    logger.info(f"Added {len(file_list)} files to {database}")

    return collection
        
        
        


def chroma_add( data:dict, database:str="default_collection", collection:Collection=None,  embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    logger = get_logger(__name__, debug=DEBUG, quiet=QUET)

    if collection is None:
        collection = get_collection(database, embedding_function)

    try:
        collection.upsert(
                documents=data.get('documents'),
                metadatas=data.get('metadatas'),
                embeddings=data.get('embeddings'),
                ids=  str(data.get('ids'))
        )
    except Exception as e:
        logger.error(e)
        raise(f"Failed to add data to {database} {e}")
    
    return "true"
 



def chroma_query(query:dict, database:str="default_collection", collection:Collection=None, embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    logger = get_logger(__name__, debug=DEBUG, quiet=QUET)
   
    if collection is None:
        collection = get_collection(database, embedding_function)
    
    if query.get("embeddings") is  None:
        embeddings = embedding_function(query.get("query_text"))
 
    try:
        result = collection.query(query_embeddings=embeddings,
                              where = query.get("where") if query.get("where") else None,
                              where_document= query.get("where_document") if query.get("where_document") else None, 
                            n_results=query.get("n_results"), 
                            include= [
                                        "metadatas",
                                        "documents",
                                        "distances"
                                ])
    except Exception as e:
        logger.error(e)
        raise Exception(f"Query failed {e}")

    return result 
    
def chroma_get(database:str, query:dict, collection:Collection=None, embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    logger.debug(f"query: {query['ids']}")
  
    collection = get_collection(name=database, embedding_function=embedding_function)
    result = collection.get(ids=query.get('ids'),
                             include=["metadatas", "documents", "embeddings"])
    return result



def add_json_string(collection:chromadb.Collection, json_string:str, embedding_function:EmbeddingFunction=embedding_functions.DefaultEmbeddingFunction()):
    

    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    doc = json.loads(json_string)
    chunks = chunk_by_size(doc, 500)
    embedded_chunks = embedding_function(chunks)
    metadata_per_chunk = [{
        "Context": "past chat session",
        "Date": current_datetime,
        "chunk_index": i,
        } for i, _ in enumerate(chunks)]
    
    collection.upsert(
        ids=[f"{current_datetime}_{i}" for i in range(len(chunks))],
        documents=chunks,
        metadatas=metadata_per_chunk,
        embeddings=embedded_chunks
    )








if __name__ == "__main__":
    
    load_env()
    
    import argparse
    argparser =  argparse.ArgumentParser()
    argparser.add_argument( "action",  help="[add, query, get]")
    argparser.add_argument( '-f',"--files",  help="files to add to chromadb", nargs='+')
    argparser.add_argument( '-c',"--collection",  help="database to add files to", type=str)
    argparser.add_argument( '-d',"--directory",  help="directory where files are", type=str)
    argparser.add_argument( '-q',"--query",  help="query to run on chromadb", type=str)
    args = argparser.parse_args()

    logger = get_logger(__name__, debug=DEBUG, quiet=QUET)
    
    ef  = embedding_functions.OpenAIEmbeddingFunction(
                    api_key=os.environ['OPENAI_API_KEY'],
                    model_name="text-embedding-ada-002"
                )


    if args.collection:
        database = args.collection
    else:
        database = "default_collection"


    if args.action == "add":
        if not args.files and not args.directory:
            logger.error("No files or directory specified")
            exit(1)

        
        if args.directory:
            files = get_file_names(args.directory)

        if args.files:
            files.append(args.files)
        add_files(files, database=database, embedding_function=ef)
        exit(0)
    
    if args.action == "query":
        if not args.query:
            logger.error("No query specified")
            exit(1)
    
        query = {
            "query_text": args.query,
            "n_results": 5
        }
        result = chroma_query(query, database=database, embedding_function=ef)
        logger.info(result)
        print("Query Results:\n")
        print(result)
        exit(0)