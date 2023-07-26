import os
import time

from fastapi import FastAPI, File, status, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from utils.file_parser import FileParser
from utils.api import ClientAPI


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.get('/')
def root() -> str:
    return 'ping'


@app.post('/file/upload')
def upload_resume(
    file: UploadFile = File(...)
) -> JSONResponse:
    _hex = hex(round(time.time() * 1e7))
    ext = file.filename.split('.')[-1]
    
    try:
        contents = file.file.read()
        write_path = f'{_hex}.{ext}'
        with open(write_path, 'wb') as f:
            f.write(contents)
            
        data = FileParser.parse_file(write_path)
        
        ClientAPI.create_db(_hex)
        for (table, df) in data.items():
            ClientAPI.create_table(_hex, table, df.columns, df.dtypes.to_list())
            ClientAPI.insert_many(_hex, table, df)
        os.remove(write_path)

        return JSONResponse(
            content=_hex, 
            status_code=status.HTTP_200_OK
        )
    except ValueError as e:
        print(e)
        return JSONResponse(
            content=str(e), 
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )
    except Exception as e:
        print(e)
        return JSONResponse(
            content='There was an error uploading the file.', 
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )
    finally:
        file.file.close()


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=10000)